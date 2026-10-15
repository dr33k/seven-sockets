package auth

import (
	"uuid"

	"github.com/Azure/go-autorest/autorest/date"
)

type Acct struct{
	Id uuid.UUID "id"
	Fname string "firstName"
	Lname string "lastName"
	Email string "email"
	Phone string "phoneNo"
	Dob date.Date "dob"
	DateCreated date.Date "dateCreated"
	DateUpdated date.Date "dateUpdated"

}