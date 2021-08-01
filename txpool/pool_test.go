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

package txpool

import (
	"testing"

	"github.com/google/btree"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestSubPoolMarkerOrder(t *testing.T) {
	require := require.New(t)
	require.Less(
		NewSubPoolMarker(true, true, true, true, false),
		NewSubPoolMarker(true, true, true, true, true),
	)
	require.Less(
		NewSubPoolMarker(true, true, true, false, true),
		NewSubPoolMarker(true, true, true, true, true),
	)
	require.Less(
		NewSubPoolMarker(true, true, true, false, true),
		NewSubPoolMarker(true, true, true, true, false),
	)
	require.Less(
		NewSubPoolMarker(false, true, true, true, true),
		NewSubPoolMarker(true, false, true, true, true),
	)
	require.Less(
		NewSubPoolMarker(false, false, false, true, true),
		NewSubPoolMarker(false, false, true, true, true),
	)
	require.Less(
		NewSubPoolMarker(false, false, true, true, false),
		NewSubPoolMarker(false, false, true, true, true),
	)
}

/*
func TestSubPoolOrder(t *testing.T) {
	sub := NewSubPool()
	sub.Add(&MetaTx{SubPool: 0b10101})
	sub.Add(&MetaTx{SubPool: 0b11110})
	sub.Add(&MetaTx{SubPool: 0b11101})
	sub.Add(&MetaTx{SubPool: 0b10001})
	require.Equal(t, uint8(0b11110), uint8(sub.Best().SubPool))
	require.Equal(t, uint8(0b10001), uint8(sub.Worst().SubPool))

	require.Equal(t, uint8(sub.Best().SubPool), uint8(sub.PopBest().SubPool))
	require.Equal(t, uint8(sub.Worst().SubPool), uint8(sub.PopWorst().SubPool))

	sub = NewSubPool()
	sub.Add(&MetaTx{SubPool: 0b00001})
	sub.Add(&MetaTx{SubPool: 0b01110})
	sub.Add(&MetaTx{SubPool: 0b01101})
	sub.Add(&MetaTx{SubPool: 0b00101})
	require.Equal(t, uint8(0b00001), uint8(sub.Worst().SubPool))
	require.Equal(t, uint8(0b01110), uint8(sub.Best().SubPool))

	require.Equal(t, uint8(sub.Worst().SubPool), uint8(sub.PopWorst().SubPool))
	require.Equal(t, uint8(sub.Best().SubPool), uint8(sub.PopBest().SubPool))
}

func TestSubPoolsPromote(t *testing.T) {
	s1 := []uint8{0b11000, 0b101, 0b111}
	s2 := []uint8{0b11000, 0b101, 0b111}
	s3 := []uint8{0b11000, 0b101, 0b111}
	pending, baseFee, queued := NewSubPool(), NewSubPool(), NewSubPool()
	for _, i := range s1 {
		pending.Add(&MetaTx{SubPool: SubPoolMarker(i & 0b11111)})
	}
	for _, i := range s2 {
		baseFee.Add(&MetaTx{SubPool: SubPoolMarker(i & 0b11111)})
	}
	for _, i := range s3 {
		queued.Add(&MetaTx{SubPool: SubPoolMarker(i & 0b11111)})
	}
	PromoteStep(pending, baseFee, queued)

	if pending.Worst() != nil {
		require.Less(t, uint8(0b01111), uint8(pending.Worst().SubPool))
	}
	if baseFee.Worst() != nil {
		require.Less(t, uint8(0b01111), uint8(baseFee.Worst().SubPool))
	}
	if queued.Worst() != nil {
		require.Less(t, uint8(0b01111), uint8(queued.Worst().SubPool))
	}
	// if limit reached, worst must be greater than X
}

//nolint
func hexToSubPool(s string) []uint8 {
	a, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	for i := range a {
		a[i] &= 0b11111
	}

	return a
}
*/

func TestProp(t *testing.T) {
	u256 := rapid.Custom(func(t *rapid.T) *uint256.Int { return uint256.NewInt(rapid.Uint64().Draw(t, "u256").(uint64)) })
	rapid.Check(t, func(t *rapid.T) {
		assert := assert.New(t)

		ids := rapid.SliceOfN(rapid.Uint64(), 3, 30).Draw(t, "ids").([]uint64)
		i := 0
		senders := map[uint64]SenderInfo{}
		for _, id := range ids {
			senders[id] = SenderInfo{
				nonce:      rapid.Uint64().Draw(t, "sender.nonce").(uint64),
				balance:    *u256.Draw(t, "sender.balance").(*uint256.Int),
				txNonce2Tx: &Nonce2Tx{btree.New(32)},
			}
		}
		i = 0
		txs := rapid.SliceOfN(rapid.Custom(func(t *rapid.T) *TxSlot {
			i++
			return &TxSlot{
				senderID: ids[i%len(ids)],
				nonce:    rapid.Uint64().Draw(t, "tx.nonce").(uint64),
				tip:      rapid.Uint64().Draw(t, "tx.tip").(uint64),
				value:    *u256.Draw(t, "tx.value").(*uint256.Int),
			}
		}), 100, 8*100).Draw(t, "s2").([]*TxSlot)

		protocolBaseFee := rapid.Uint64().Draw(t, "protocolBaseFee").(uint64)
		blockBaseFee := rapid.Uint64().Draw(t, "blockBaseFee").(uint64)

		pending, baseFee, queued := NewSubPool(), NewSubPool(), NewSubPool()
		OnNewBlocks(senders, txs, protocolBaseFee, blockBaseFee, pending, baseFee, queued)

		best, worst := pending.Best(), pending.Worst()
		assert.LessOrEqual(pending.Len(), PendingSubPoolLimit)
		assert.False(worst != nil && best == nil)
		assert.False(worst == nil && best != nil)
		if worst != nil && worst.SubPool < 0b11110 {
			t.Fatalf("pending worst too small %b", worst.SubPool)
		}
		iterateSubPoolUnordered(pending, func(tx *MetaTx) {
			i := tx.Tx
			assert.GreaterOrEqual(i.nonce, senders[i.senderID].nonce)
			if tx.SubPool&EnoughBalance > 0 {
				assert.True(tx.SenderHasEnoughBalance)
			}

			need := uint256.NewInt(i.gas)
			need = need.Mul(need, uint256.NewInt(i.feeCap))
			assert.GreaterOrEqual(uint256.NewInt(protocolBaseFee), need.Add(need, &i.value))
			assert.GreaterOrEqual(uint256.NewInt(blockBaseFee), need.Add(need, &i.value))
		})

		best, worst = baseFee.Best(), baseFee.Worst()

		assert.False(worst != nil && best == nil)
		assert.False(worst == nil && best != nil)
		assert.LessOrEqual(baseFee.Len(), BaseFeeSubPoolLimit)
		if worst != nil && worst.SubPool < 0b11100 {
			t.Fatalf("baseFee worst too small %b", worst.SubPool)
		}
		iterateSubPoolUnordered(baseFee, func(tx *MetaTx) {
			i := tx.Tx
			assert.GreaterOrEqual(i.nonce, senders[i.senderID].nonce)
			if tx.SubPool&EnoughBalance > 0 {
				assert.True(tx.SenderHasEnoughBalance)
			}

			need := uint256.NewInt(i.gas)
			need = need.Mul(need, uint256.NewInt(i.feeCap))
			assert.GreaterOrEqual(uint256.NewInt(protocolBaseFee), need.Add(need, &i.value))
			assert.GreaterOrEqual(uint256.NewInt(blockBaseFee), need.Add(need, &i.value))
		})

		best, worst = queued.Best(), queued.Worst()
		assert.LessOrEqual(queued.Len(), QueuedSubPoolLimit)
		assert.False(worst != nil && best == nil)
		assert.False(worst == nil && best != nil)
		if worst != nil && worst.SubPool < 0b10000 {
			t.Fatalf("queued worst too small %b", worst.SubPool)
		}
		iterateSubPoolUnordered(queued, func(tx *MetaTx) {
			i := tx.Tx
			assert.GreaterOrEqual(i.nonce, senders[i.senderID].nonce)
			if tx.SubPool&EnoughBalance > 0 {
				assert.True(tx.SenderHasEnoughBalance)
			}

			need := uint256.NewInt(i.gas)
			need = need.Mul(need, uint256.NewInt(i.feeCap))
			assert.GreaterOrEqual(uint256.NewInt(protocolBaseFee), need.Add(need, &i.value))
			assert.GreaterOrEqual(uint256.NewInt(blockBaseFee), need.Add(need, &i.value))
		})

	})
}
