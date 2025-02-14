//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/holmes89/dynamo
//

package ddb

import (
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/fogfish/golem/pure/hseq"
	"github.com/holmes89/dynamo"
)

/*
Schema is utility that decodes type into projection expression
*/
type Schema[T dynamo.Thing] struct {
	ExpectedAttributeNames map[string]string
	Projection             *string
}

func NewSchema[T dynamo.Thing]() *Schema[T] {
	seq := hseq.FMap(
		hseq.Generic[T](),
		func(t hseq.Type[T]) string {
			name := t.StructField.Tag.Get("dynamodbav")
			return strings.Split(name, ",")[0]
		},
	)

	names := make(map[string]string, len(seq))
	schema := make([]string, len(seq))

	for i, x := range seq {
		name := "#__" + x + "__"
		names[name] = x
		schema[i] = name
	}

	return &Schema[T]{
		ExpectedAttributeNames: names,
		Projection:             aws.String(strings.Join(schema, ", ")),
	}
}
