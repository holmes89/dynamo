//
// Copyright (C) 2022 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/holmes89/dynamo
//

package s3_test

import (
	"testing"

	"github.com/fogfish/curie"
	"github.com/fogfish/it"
	"github.com/holmes89/dynamo/internal/dynamotest"
	"github.com/holmes89/dynamo/internal/s3"
)

func TestMerge(t *testing.T) {
	a := dynamotest.Person{
		Prefix: curie.New("dead:beef"),
		Suffix: curie.New("1"),
		Name:   "Verner Pleishner",
	}

	b := dynamotest.Person{
		Age:     64,
		Address: "Blumenstrasse 14, Berne, 3013",
	}

	c := dynamotest.Person{
		Prefix:  curie.New("dead:beef"),
		Suffix:  curie.New("1"),
		Name:    "Verner Pleishner",
		Age:     64,
		Address: "Blumenstrasse 14, Berne, 3013",
	}

	t.Run("Values", func(t *testing.T) {
		schema := s3.NewSchema[dynamotest.Person]()
		it.Ok(t).
			If(schema.Merge(a, b)).Should().Equal(c)
	})

	t.Run("Pointers", func(t *testing.T) {
		schema := s3.NewSchema[*dynamotest.Person]()
		it.Ok(t).
			If(schema.Merge(&a, &b)).Should().Equal(&c)
	})
}
