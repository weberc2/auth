package types

import (
	"encoding/json"
	"fmt"
)

type NotificationType string

const (
	NotificationTypeRegister       NotificationType = "REGISTER"
	NotificationTypeForgotPassword NotificationType = "FORGOT_PASSWORD"
)

type Notification struct {
	Type  NotificationType
	User  UserID
	Email string
	Token string
}

type NotificationService interface {
	Notify(*Notification) error
}

func (wanted *Notification) Compare(found *Notification) error {
	if wanted == nil && found == nil {
		return nil
	}

	if wanted != nil && found == nil {
		return fmt.Errorf("Notification: unexpected `nil`")
	}

	if wanted == nil && found != nil {
		return fmt.Errorf("Notification: wanted `nil`; found not-nil")
	}

	if wanted.Type != found.Type {
		return fmt.Errorf(
			"Notification.Type: wanted `%s`; found `%s`",
			wanted.Type,
			found.Type,
		)
	}

	if wanted.User != found.User {
		return fmt.Errorf(
			"Notification.User: wanted `%s`; found `%s`",
			wanted.User,
			found.User,
		)
	}

	if wanted.Email != found.Email {
		return fmt.Errorf(
			"Notification.Email: wanted `%s`; found `%s`",
			wanted.Email,
			found.Email,
		)
	}

	if wanted.Token != found.Token {
		return fmt.Errorf(
			"Notification.Token: wanted `%s`; found `%s`",
			wanted.Token,
			found.Token,
		)
	}

	return nil
}

func (n *Notification) CompareData(data []byte) error {
	var other Notification
	if err := json.Unmarshal(data, &other); err != nil {
		return fmt.Errorf("unmarshaling `Notification`: %w", err)
	}
	return n.Compare(&other)
}
