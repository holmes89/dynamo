//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/holmes89/dynamo
//

//
// The file declares sequence type (traversal) for s3
//

package s3

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/curie"
	"github.com/holmes89/dynamo"
)

type cursor struct{ hashKey, sortKey string }

func (c cursor) HashKey() curie.IRI { return curie.IRI(c.hashKey) }
func (c cursor) SortKey() curie.IRI { return curie.IRI(c.sortKey) }

// seq is an iterator over matched results
type seq[T dynamo.Thing] struct {
	ctx    context.Context
	db     *Storage[T]
	q      *s3.ListObjectsV2Input
	at     int
	items  []*string
	stream bool
	err    error
}

func newSeq[T dynamo.Thing](
	ctx context.Context,
	db *Storage[T],
	q *s3.ListObjectsV2Input,
	err error,
) *seq[T] {
	return &seq[T]{
		ctx:    ctx,
		db:     db,
		q:      q,
		at:     0,
		items:  nil,
		stream: true,
		err:    err,
	}
}

func (seq *seq[T]) maybeSeed() error {
	if !seq.stream {
		return errEndOfStream()
	}

	return seq.seed()
}

func (seq *seq[T]) seed() error {
	if seq.items != nil && seq.q.StartAfter == nil {
		return errEndOfStream()
	}

	val, err := seq.db.Service.ListObjectsV2(seq.ctx, seq.q)
	if err != nil {
		seq.err = err
		return errServiceIO(err)
	}

	if val.KeyCount == 0 {
		return errEndOfStream()
	}

	items := make([]*string, 0)
	for _, x := range val.Contents {
		items = append(items, x.Key)
	}

	seq.at = 0
	seq.items = items
	if len(items) > 0 && val.NextContinuationToken != nil {
		seq.q.StartAfter = items[len(items)-1]
	}

	if val.NextContinuationToken == nil {
		seq.q.StartAfter = nil
	}

	return nil
}

// FMap transforms sequence
func (seq *seq[T]) FMap(f func(T) error) error {
	for seq.Tail() {
		head, err := seq.Head()
		if err != nil {
			return err
		}

		if err := f(head); err != nil {
			return errProcessEntity(err, head)
		}
	}
	return seq.err
}

// Head selects the first element of matched collection.
func (seq *seq[T]) Head() (T, error) {
	if seq.items == nil {
		if err := seq.seed(); err != nil {
			return seq.db.undefined,
				fmt.Errorf("can't seed head of stream: %w", err)
		}
	}

	req := &s3.GetObjectInput{
		Bucket: seq.db.Bucket,
		Key:    seq.items[seq.at],
	}
	val, err := seq.db.Service.GetObject(seq.ctx, req)
	if err != nil {
		return seq.db.undefined, errServiceIO(err)
	}

	var head T
	err = json.NewDecoder(val.Body).Decode(&head)
	if err != nil {
		return seq.db.undefined, errInvalidEntity(err)
	}

	return head, nil
}

// Tail selects the all elements except the first one
func (seq *seq[T]) Tail() bool {
	seq.at++

	switch {
	case seq.err != nil:
		return false
	case seq.items == nil:
		err := seq.seed()
		return err == nil
	case seq.err == nil && seq.at >= len(seq.items):
		err := seq.maybeSeed()
		return err == nil
	default:
		return true
	}
}

// Cursor is the global position in the sequence
func (seq *seq[T]) Cursor() dynamo.Thing {
	if seq.q.StartAfter != nil {
		return &cursor{hashKey: *seq.q.StartAfter}
	}
	return &cursor{}
}

// Error indicates if any error appears during I/O
func (seq *seq[T]) Error() error {
	return seq.err
}

// Limit sequence to N elements
func (seq *seq[T]) Limit(n int) dynamo.Seq[T] {
	seq.q.MaxKeys = int32(n)
	seq.stream = false
	return seq
}

// Continue limited sequence from the cursor
func (seq *seq[T]) Continue(key dynamo.Thing) dynamo.Seq[T] {
	// Note: s3 cursor supports only HashKey
	prefix := key.HashKey()

	if prefix != "" {
		seq.q.StartAfter = aws.String(string(prefix))
	}

	return seq
}

// Reverse order of sequence
func (seq *seq[T]) Reverse() dynamo.Seq[T] {
	return seq
}
