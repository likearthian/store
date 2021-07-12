package store

import "errors"

var (
	ErrKeyAlreadyExists = errors.New("key already exists")
	ErrKeynotFound = errors.New("key not found")
	ErrNoRow = errors.New("no row")
)
