package auth

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/weberc2/auth/pkg/auth/types"
)

type ConsoleNotificationService struct{}

func (cns ConsoleNotificationService) Notify(
	u types.UserID,
	t uuid.UUID,
) error {
	data, err := json.Marshal(struct {
		User  types.UserID `json:"user"`
		Token string       `json:"token"`
	}{
		User:  u,
		Token: t.String(),
	})
	if err != nil {
		return err
	}
	_, err = fmt.Printf("%s\n", data)
	return err
}
