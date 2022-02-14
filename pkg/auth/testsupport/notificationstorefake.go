package testsupport

import "github.com/weberc2/auth/pkg/auth/types"

type NotificationServiceFake struct {
	Notifications []*types.Notification
}

func (nsf *NotificationServiceFake) Notify(n *types.Notification) error {
	nsf.Notifications = append(nsf.Notifications, n)
	return nil
}
