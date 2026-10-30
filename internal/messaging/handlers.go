package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
	"uuid"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type Msg struct {
	Typ   int
	Bytes []byte
}

type FileAttachment struct {
	FileID   uuid.UUID `json:"file_id"`
	FileName string    `json:"file_name"`
	MimeType string    `json:"mime_type"`
	Size     int64     `json:"size"`
}

type Payload struct{
	To        uuid.UUID        `json:"to"`
	Content   string           `json:"content"`
	Attachments []FileAttachment `json:"attachments,omitempty"`
	Timestamp time.Time        `json:"timestamp"`
}

func HandleWS(ctx *gin.Context) {
	const handlerName = "ws-handler"

	//1. Upgrade HTTP connection to a websocket connection
	conn, err := upgrader.Upgrade(ctx.Writer, ctx.Request, nil)
	if err != nil {
		log.Printf("[%s] Failed to upgrade connection: %v", handlerName, err)
		return
	}
	defer conn.Close()

	log.Printf("[%s] New client connected successfully", handlerName)

	msgChan := make(chan *Msg, 256)
	errChan := make(chan *Msg, 256)

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
				if !ok{
					log.Printf("[%s] Error channel closed", workerName)
					return
				}

				err := conn.WriteMessage(errMsg.Typ, errMsg.Bytes)
				if err != nil {
					log.Printf("[%s] Failed to write error message from receiver channel to WS: %v", workerName, err)
					return
				}

			case msg, ok := <-msgChan:
				if !ok{
					log.Printf("[%s] Message channel closed", workerName)
					return
				}
				
				err := conn.WriteMessage(msg.Typ, msg.Bytes)
				if err != nil {
					log.Printf("[%s] Failed to write message from receiver channel to WS: %v", workerName, err)
					return
				}

				log.Printf("[%s] Message recieved from message channel and written to WS", workerName)
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

		var payload Payload
		if err := json.Unmarshal(msgBytes, &payload); err != nil {
			select {
				case errChan <- &Msg{Typ: msgTyp, Bytes: fmt.Appendf(msgBytes, ": [%s] Invalid message format", handlerName)}:
				default:
					log.Printf("[%s] Error channel output buffer full, dropping message", handlerName)
        	}
			log.Printf("[%s] Invalid message format: %v", handlerName, err)

			continue
		}

		// Now you can inspect it cleanly:
		if len(payload.Attachments) > 0 {
			// Route to Object Storage / File Processing Queue
		} else {
			// Route straight to Message Persistence Queue
		}

		log.Printf("[%s] Passed message: %s to message channel", handlerName, string(msgBytes))

	}

	//Cancel writer context
	cancel()
}
