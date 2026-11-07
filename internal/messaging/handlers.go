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
	const handlerName = "open-ws-handler"

	//Retrieve Auth Context
	authCtx, err := auth.GetAuthCtx(ctx)
	if err != nil {
		return
	}

	//1. Upgrade HTTP connection to a websocket connection
	conn, err := upgrader.Upgrade(ctx.Writer, ctx.Request, nil)
	if err != nil {
		log.Printf("[%s] Failed to upgrade connection: %v", handlerName, err)
		return
	}
	defer conn.Close()

	log.Printf("[%s] New client connected successfully", handlerName)

	msgChan := make(chan RmqMsg, 256)
	errChan := make(chan RmqMsg, 256)

	// Create a local context/cancel to coordinate clean shutdown between reader and writer
	writeCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//2. Write loop in spawned goroutine
	go func() {
		const workerName = "ws-writer"
		for {
			select {
			case <-writeCtx.Done():
				log.Printf("[%s] WS Writer shutting down", workerName)
				return

			case errMsg, ok := <-errChan:
				if !ok {
					log.Printf("[%s] Error channel closed", workerName)
					return
				}

				err := conn.WriteMessage(errMsg.Typ, errMsg.Bytes)
				if err != nil {
					log.Printf("[%s] Failed to write error message from receiver channel to WS: %v", workerName, err)
					return
				}

			case msg, ok := <-msgChan:
				if !ok {
					log.Printf("[%s] Message channel closed", workerName)
					return
				}

				err := conn.WriteMessage(msg.Typ, msg.Bytes)
				if err != nil {
					log.Printf("[%s] Failed to write message from receiver channel to WS: %v", workerName, err)
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
				log.Printf("[%s] Connection error: %v", handlerName, err)
			} else {
				log.Printf("[%s] Client disconnected", handlerName)
			}
			break
		}

		//Unmarshal
		var payload Payload
		if err := json.Unmarshal(msgBytes, &payload); err != nil {
			writeErrorMsgToWS(err, errChan, msgTyp, msgBytes, handlerName)
			continue
		}

		now := time.Now()
		payload.Timestamp = now

		// Validate the struct based on struct tags
		if err := validate.Struct(&payload); err != nil {
			writeErrorMsgToWS(err, errChan, msgTyp, msgBytes, handlerName)
			continue
		}

		if len(payload.Attachments) > 0 {
			// Route to Object Storage / File Processing Queue
			err = rmq.PubEvent("", "", rmq.Event{
				AcctID:    authCtx.Account.Id.String(),
				Type:      "OBJ_STORE",
				Payload:   payload,
				Timestamp: now.Unix(),
			})

		} else {
			//Validate Empty Content
			if strings.TrimSpace(payload.Content) == "" {
				writeErrorMsgToWS(err, errChan, msgTyp, msgBytes, handlerName)
			}

			// Route straight to Message Persistence Queue
			err = rmq.PubEvent(env.RmqMsgXName, env.RmqMsgRoutingKey, rmq.Event{
				AcctID:    authCtx.Account.Id.String(),
				Type:      "MSG_PERSIST",
				Payload:   payload,
				Timestamp: now.Unix(),
			})
		}

		if err != nil {
			log.Println(err.Error())
			ctx.JSON(http.StatusInternalServerError, gin.H{"msg": "Unable to send new message event"})
			return
		}

		log.Printf("[%s] Passed message: %s to message channel", handlerName, string(msgBytes))
	}

	//Cancel writer context
	cancel()
}

func PersistMsg(event *rmq.Event) error {
	const handlerName = "persist-msg-handler"

	if mongodb.DB == nil {
		return fmt.Errorf("[%s] Uninitialized Mongo DB Pool", handlerName)
	}

	switch event.Type {
	case "MSG_PERSIST":
		{
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			payload, err := unmarshalPayloadToBytes(event.Payload)
			if err != nil {
				return fmt.Errorf("[%s] Invalid Message Payload %v", handlerName, event)
			}

			collection := mongodb.DB.Collection("messages")

			_, err = collection.InsertOne(ctx, payload)
			if err != nil {
				return fmt.Errorf("[%s] Failed to persist message to DB: %w", handlerName, err)
			}

			log.Printf("[%s] Successfully persisted message to DB", handlerName)
			return nil
		}
	default:
		return fmt.Errorf("[%s] Invalid event type for Message persistence: %s", handlerName, event.Type)
	}
}

func unmarshalPayloadToBytes(p any) (*Payload, error) {
	// 1. Marshal the generic payload back into JSON bytes
	payloadBytes, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event payload: %w", err)
	}

	// 2. Unmarshal into struct
	var payload Payload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return nil, fmt.Errorf("failed to decode into Payload struct: %w", err)
	}

	return &payload, nil
}

func writeErrorMsgToWS(err error, errChan chan RmqMsg, msgTyp int, msgBytes []byte, handlerName string) {
	log.Printf("[%s] Invalid message format: %v", handlerName, err)
	select {
	case errChan <- RmqMsg{Typ: msgTyp, Bytes: fmt.Appendf(msgBytes, ": Invalid message format")}:
	default:
		log.Printf("[%s] Error channel output buffer full, dropping message", handlerName)
	}
}
