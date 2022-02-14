package auth

import "github.com/weberc2/auth/pkg/auth/types"

type MemUserStore struct {
	Entries []*types.UserEntry
}

func (mus *MemUserStore) Create(entry *types.UserEntry) error {
	for _, e := range mus.Entries {
		if e.User == entry.User {
			return ErrUserExists
		}
	}
	mus.Entries = append(mus.Entries, entry)
	return nil
}

func (mus *MemUserStore) Update(entry *types.UserEntry) error {
	for i, e := range mus.Entries {
		if e.User == entry.User {
			mus.Entries[i] = entry
			return nil
		}
	}
	return types.ErrUserNotFound
}

func (mus *MemUserStore) Get(user types.UserID) (*types.UserEntry, error) {
	for _, e := range mus.Entries {
		if e.User == user {
			return e, nil
		}
	}
	return nil, types.ErrUserNotFound
}
