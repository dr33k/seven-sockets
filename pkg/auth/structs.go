package auth

import (
	"time"
	"uuid"

	"github.com/Azure/go-autorest/autorest/date"
	"github.com/golang-jwt/jwt/v5"
)

type Acct struct{
	Id uuid.UUID `json:"id"`
	Fname string `json:"firstName"`
	Lname string `json:"lastName"`
	Email string `json:"email"`
	Phone string `json:"phoneNo"`
	Dob date.Date `json:"dob"`
	DateCreated time.Time `json:"dateCreated"`
	DateUpdated time.Time `json:"dateUpdated"`

}

type AuthContext struct{
	Account Acct `json:"principal"`
	Tenant string `json:"tenant"`
	jwt.RegisteredClaims
}