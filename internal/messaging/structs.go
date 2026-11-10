package messaging

import (
	"time"

	"github.com/google/uuid"
)

type Msg struct {
	Typ   int
	Bytes []byte
}

type FileAttachment struct {
	FileID   uuid.UUID `json:"file_id" bson:"file_id"`
	FileName string    `json:"file_name" bson:"file_name"`
	MimeType string    `json:"mime_type" bson:"mime_type"`
	Size     int64     `json:"size" bson:"size"`
}

type MsgPayload struct {
	MsgID   uuid.UUID `json:"msg_id" bson:"_id" validate:"required"`
	To      uuid.UUID `json:"to" bson:"to" validate:"required"`
	Content string    `json:"content" bson:"content" validate:"required"`
	Status string	  `json:"status" bson:"status"`
	Attachments []FileAttachment `json:"attachments,omitempty" bson:"attachments,omitempty" validate:"omitempty"`
	SentAt      time.Time        `json:"sent_at" bson:"sent_at" validate:"required"`
	PersistedAt time.Time        `json:"persisted_at" bson:"persisted_at"`
}

type MsgStatusResponsePayload struct {
	MsgId  uuid.UUID `json:"msg_id" validate:"required"`
	Value  string    `json:"value"`
	SentAt time.Time `json:"timestamp"`
}
