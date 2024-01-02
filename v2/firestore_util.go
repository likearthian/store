package store

import (
	"reflect"

	"cloud.google.com/go/firestore"
)

func parseFirestoreDoc[T any](dest *T, snap *firestore.DocumentSnapshot, docIDFieldPos int) error {
	err := snap.DataTo(dest)
	if err != nil {
		return err
	}

	if docIDFieldPos >= 0 {
		v := reflect.ValueOf(dest).Elem()
		v.Field(docIDFieldPos).SetString(snap.Ref.ID)
	}

	return nil
}

type firestoreIterator[T any] struct {
	iter          *firestore.DocumentIterator
	docIDFieldPos int
}

func (fi *firestoreIterator[T]) Next() (*T, error) {
	doc, err := fi.iter.Next()
	if err != nil {
		return nil, err
	}

	var data T
	if err = parseFirestoreDoc(&data, doc, fi.docIDFieldPos); err != nil {
		return nil, err
	}

	return &data, nil
}

func (fi *firestoreIterator[T]) Close() error {
	fi.iter.Stop()
	return nil
}
