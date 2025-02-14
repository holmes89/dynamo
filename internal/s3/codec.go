//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/holmes89/dynamo
//

package s3

import (
	"github.com/fogfish/curie"
	"github.com/holmes89/dynamo"
)

/*
Codec is utility to encode/decode objects to s3 representation
*/
type Codec[T dynamo.Thing] struct {
	prefixes curie.Prefixes
}

func NewCodec[T dynamo.Thing](prefixes curie.Prefixes) *Codec[T] {
	if prefixes == nil {
		return &Codec[T]{prefixes: curie.Namespaces{}}
	}

	return &Codec[T]{prefixes: prefixes}
}

func (codec Codec[T]) EncodeKey(key T) string {
	hkey := curie.URI(codec.prefixes, key.HashKey())
	skey := curie.URI(codec.prefixes, key.SortKey())

	if skey == "" {
		return hkey
	}

	return hkey + "/" + skey
}
