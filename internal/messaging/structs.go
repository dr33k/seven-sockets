package messaging

import (
	"uuid"
)

type RmqMsg struct {
	Typ   int
	Bytes []byte
}

type FileAttachment struct {
	FileID   uuid.UUID `json:"file_id"`
	FileName string    `json:"file_name"`
	MimeType string    `json:"mime_type"`
	Size     int64     `json:"size"`
}

type Payload struct {
	To          uuid.UUID        `json:"to"`
	Content     string           `json:"content"`
	Attachments []FileAttachment `json:"attachments,omitempty"`
}
