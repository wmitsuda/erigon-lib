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

type Aggregate struct {
	env *mdbx.Env
}

// OpenAggregateRO opens a state aggregate from a read only MDBX environment (database)
func OpenAggregageRO(path string) (*Aggregate, error) {
	env, err := mdbx.NewEnv()
	if err != nil {
		return nil, err
	}
	var flags uint = mdbx.Readonly | mdbx.NoReadahead | mdbx.Coalesce | mdbx.Durable
	err = env.Open(path, flags, 0664)
	if err != nil {
		return nil, fmt.Errorf("opening RO aggregate %s: %w", path, err)
	}
	return &Aggregate{env: env}, nil
}
