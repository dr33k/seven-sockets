package messaging

import (
	"time"
	"github.com/google/uuid"
)

type RmqMsg struct {
	Typ   int
	Bytes []byte
}

type FileAttachment struct {
	FileID   uuid.UUID `json:"file_id" bson:"file_id"`
	FileName string    `json:"file_name" bson:"file_name"`
	MimeType string    `json:"mime_type" bson:"mime_type"`
	Size     int64     `json:"size" bson:"size"`
}

type Payload struct {
	To          uuid.UUID        `json:"to" bson:"to" validate:"required"`         // Cannot be empty/zero UUID
	Content     string           `json:"content" bson:"content" validate:"required"` // Cannot be an empty string ""
	Attachments []FileAttachment `json:"attachments,omitempty" bson:"attachments,omitempty" validate:"omitempty"`
	Timestamp   time.Time        `json:"timestamp" bson:"timestamp" validate:"required"`
}