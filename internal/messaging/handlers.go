package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"

	"github.com/gorilla/websocket"
	"seven.com/sockets/pkg/auth"
	"seven.com/sockets/pkg/db/mongodb"
	"seven.com/sockets/pkg/db/rmq"
	"seven.com/sockets/pkg/env"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

var validate = validator.New()

func OpenWS(ctx *gin.Context) {
	const handler = "open-ws-handler"

	//Retrieve Auth Context
	authCtx, err := auth.GetAuthCtx(ctx)
	if err != nil {
		return
	}

	//1. Upgrade HTTP connection to a websocket connection
	conn, err := upgrader.Upgrade(ctx.Writer, ctx.Request, nil)
	if err != nil {
		log.Printf("[%s] Failed to upgrade connection: %v", handler, err)
		return
	}
	defer conn.Close()

	log.Printf("[%s] New client connected successfully", handler)

	msgChan := make(chan *Msg, 256)
	errChan := make(chan *Msg, 256)

	// Register client in the global WS connection hub
	WSHub.Register(authCtx.ID, msgChan, errChan)
	defer WSHub.Unregister(authCtx.ID) // Clean up on disconnect

	// Create a local context/cancel to coordinate clean shutdown between reader and writer
	writeCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//2. Write loop in spawned goroutine
	go func() {
		const worker = "ws-writer"
		for {
			select {
			case <-writeCtx.Done():
				log.Printf("[%s] WS Writer shutting down", worker)
				return

			case errMsg, ok := <-errChan:
				if !ok {
					log.Printf("[%s] Error channel closed", worker)
					return
				}

				err := conn.WriteMessage(errMsg.Typ, errMsg.Bytes)
				if err != nil {
					log.Printf("[%s] Failed to write error message from receiver channel to WS: %v", worker, err)
					return
				}

			case msg, ok := <-msgChan:
				if !ok {
					log.Printf("[%s] Message channel closed", worker)
					return
				}

				err := conn.WriteMessage(msg.Typ, msg.Bytes)
				if err != nil {
					log.Printf("[%s] Failed to write message from receiver channel to WS: %v", worker, err)
					return
				}
			}
		}
	}()

	//3. Read Loop in handler
	for {
		//Read message from client
		msgTyp, msgBytes, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("[%s] Connection error: %v", handler, err)
			} else {
				log.Printf("[%s] Client disconnected", handler)
			}
			break
		}

		//Unmarshal
		var payload MsgPayload
		if err := json.Unmarshal(msgBytes, &payload); err != nil {
			writeErrorMsgToWS(handler, err, errChan, msgTyp, msgBytes)
			continue
		}

		// Validate the struct based on struct tags
		if err := validate.Struct(&payload); err != nil {
			writeErrorMsgToWS(handler, err, errChan, msgTyp, msgBytes)
			continue
		}

		if len(payload.Attachments) > 0 {
			// QUEUE: Object Storage
			err = rmq.PubEvent("", "", rmq.Event{
				AcctID:    authCtx.Account.Id.String(),
				Type:      "OBJ_STORE",
				Payload:   payload,
				Timestamp: time.Now().Unix(),
			})

		} else {
			//Validate Empty Content
			if strings.TrimSpace(payload.Content) == "" {
				writeErrorMsgToWS(handler, fmt.Errorf("Empty content"), errChan, msgTyp, msgBytes)
				continue
			}

			// QUEUE: Msg Persistence
			err = rmq.PubEvent(env.RmqMsgXName, env.RmqMsgRoutingKey, rmq.Event{
				AcctID:    authCtx.Account.Id.String(),
				Type:      "MSG_CREATE",
				Payload:   payload,
				Timestamp: time.Now().Unix(),
			})
		}

		if err != nil {
			log.Println(err.Error())
			ctx.JSON(http.StatusInternalServerError, gin.H{"msg": "Unable to send new message event"})
			return
		}

		log.Printf("[%s] Passed message: %s to downstream handlers", handler, string(msgBytes))
	}

	//Cancel writer context
	cancel()
}

func PersistMsg(event *rmq.Event) error {
	const handler = "persist-msg-handler"

	if mongodb.DB == nil {
		return fmt.Errorf("[%s] Uninitialized Mongo DB Pool", handler)
	}

	switch event.Type {
	case "MSG_CREATE":
		{
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			payload, err := castMsgPayload(event.Payload)
			if err != nil {
				return fmt.Errorf("[%s] Invalid Message Payload %v", handler, event)
			}

			payload.Status = "CREATED"
			payload.PersistedAt = time.Now().UTC()

			collection := mongodb.DB.Collection("messages")

			_, err = collection.InsertOne(ctx, payload)
			if err != nil {
				return fmt.Errorf("[%s] Failed to persist message to DB: %w", handler, err)
			}

			log.Printf("[%s] Successfully persisted message to DB", handler)

			// QUEUE: Message Status
			err = rmq.PubEvent(env.RmqStatusXName, env.RmqStatusRoutingKey, rmq.Event{
				AcctID:    event.AcctID,
				Type:      "MSG_CREATED",
				Payload:   payload,
				Timestamp: time.Now().Unix(),
			})

			return nil
		}
	default:
		return fmt.Errorf("[%s] Invalid event type for Message persistence: %s", handler, event.Type)
	}
}

func castMsgPayload(p any) (*MsgPayload, error) {
	// 1. Marshal the generic payload back into JSON bytes
	payloadBytes, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event payload: %w", err)
	}

	// 2. Unmarshal into struct
	var payload MsgPayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return nil, fmt.Errorf("failed to decode into Payload struct: %w", err)
	}

	return &payload, nil
}

func writeErrorMsgToWS(handler string, err error, errChan chan *Msg, msgTyp int, msgBytes []byte) {
	log.Printf("[%s] Invalid message format: %v", handler, err)
	select {
	case errChan <- &Msg{Typ: msgTyp, Bytes: fmt.Appendf(msgBytes, ": Invalid message format")}:
	default:
		log.Printf("[%s] Error channel output buffer full, dropping message", handler)
	}
}

func NotifySender(event *rmq.Event) error {
	const handler = "notify-sender-handler"

	switch event.Type {
	case "MSG_CREATED":

		msgPyld, err := castMsgPayload(event.Payload)
		if err != nil {
			return fmt.Errorf("[%s] Unable to cast msg payload: %w", handler, err)
		}

		statusPyld := MsgStatusResponsePayload{
			MsgId:  msgPyld.MsgID,
			Value:  msgPyld.Status,
			SentAt: msgPyld.SentAt,
		}

		bytes, err := json.Marshal(statusPyld)
		if err != nil {
			return fmt.Errorf("[%s] Failed to marshal status payload: %w", handler, err)
		}

		msg := &Msg{
			Typ:   1,
			Bytes: bytes,
		}

		success := WSHub.PushMsgChan(event.AcctID, msg)
		if !success {
			log.Printf("[%s] User offline. Message status update to client will occur upon reconnection", handler)
		}

		return nil
	default:
		return fmt.Errorf("[%s] Invalid event type for Message persistence: %s", handler, event.Type)
	}
}
