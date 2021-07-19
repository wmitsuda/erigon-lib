/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package state

import (
	"fmt"

	"github.com/torquem-ch/mdbx-go/mdbx"
)

type AggregateRO struct {
	env *mdbx.Env
}

// OpenAggregateRO opens a state aggregate from a read only MDBX environment (database)
func OpenAggregageRO(path string) (*AggregateRO, error) {
	env, err := mdbx.NewEnv()
	if err != nil {
		return nil, err
	}
	var flags uint = mdbx.Readonly | mdbx.NoReadahead | mdbx.Coalesce | mdbx.Durable
	err = env.Open(path, flags, 0664)
	if err != nil {
		return nil, fmt.Errorf("opening RO aggregate %s: %w", path, err)
	}
	return &AggregateRO{env: env}, nil
}

func (aro *AggregateRO) Close() {
	aro.env.Close()
}

type AggregateBuilder struct {
	env    *mdbx.Env
	tx     *mdbx.Txn
	sDbi   mdbx.DBI
	cursor *mdbx.Cursor
}

func BuildAggregate(path string) (*AggregateBuilder, error) {
	env, err := mdbx.NewEnv()
	if err != nil {
		return nil, err
	}
	var flags uint = mdbx.NoReadahead | mdbx.Coalesce | mdbx.Durable
	err = env.Open(path, flags, 0664)
	if err != nil {
		return nil, fmt.Errorf("creating aggregate builder %s: %w", path, err)
	}
	tx, err := env.BeginTxn(nil, 0)
	if err != nil {
		return nil, fmt.Errorf("open rw transaction for aggregate builder %s: %w", path, err)
	}
	dbi, err := tx.OpenDBI("S", mdbx.DBAccede, nil, nil)
	if err != nil && !mdbx.IsNotFound(err) {
		return nil, fmt.Errorf("create table S: %w", err)
	}
	if err == nil {
		return nil, fmt.Errorf("table S already exists")
	}
	flags = mdbx.Create | mdbx.DupSort

	if dbi, err = tx.OpenDBI("S", flags, nil, nil); err != nil {
		return nil, fmt.Errorf("create table S: %w", err)
	}
	var cursor *mdbx.Cursor
	if cursor, err = tx.OpenCursor(dbi); err != nil {
		return nil, fmt.Errorf("open cursor for S: %w", err)
	}
	return &AggregateBuilder{env: env, tx: tx, sDbi: dbi, cursor: cursor}, nil
}

func (ab *AggregateBuilder) Append(key, value []byte) error {
	return ab.cursor.Put(key, value, mdbx.Append)
}

func (ab *AggregateBuilder) AppendComposite(key, value []byte) error {
	return ab.cursor.Put(key, value, mdbx.AppendDup)
}

func (ab *AggregateBuilder) Close() error {
	ab.cursor.Close()
	if _, err := ab.tx.Commit(); err != nil {
		return err
	}
	ab.env.Close()
	return nil
}
