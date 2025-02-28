package commitment

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"golang.org/x/crypto/sha3"
	"golang.org/x/exp/slices"
)

type UpdateFlags uint8

const (
	CODE_UPDATE    UpdateFlags = 1
	DELETE_UPDATE  UpdateFlags = 2
	BALANCE_UPDATE UpdateFlags = 4
	NONCE_UPDATE   UpdateFlags = 8
	STORAGE_UPDATE UpdateFlags = 16
)

func (uf UpdateFlags) String() string {
	var sb strings.Builder
	if uf == DELETE_UPDATE {
		sb.WriteString("Delete")
	} else {
		if uf&BALANCE_UPDATE != 0 {
			sb.WriteString("+Balance")
		}
		if uf&NONCE_UPDATE != 0 {
			sb.WriteString("+Nonce")
		}
		if uf&CODE_UPDATE != 0 {
			sb.WriteString("+Code")
		}
		if uf&STORAGE_UPDATE != 0 {
			sb.WriteString("+Storage")
		}
	}
	return sb.String()
}

type Update struct {
	Flags             UpdateFlags
	Balance           uint256.Int
	Nonce             uint64
	CodeHashOrStorage [length.Hash]byte
	ValLength         int
}

func (u *Update) DecodeForStorage(enc []byte) {
	u.Nonce = 0
	u.Balance.Clear()
	copy(u.CodeHashOrStorage[:], EmptyCodeHash)

	pos := 0
	nonceBytes := int(enc[pos])
	pos++
	if nonceBytes > 0 {
		u.Nonce = bytesToUint64(enc[pos : pos+nonceBytes])
		pos += nonceBytes
	}
	balanceBytes := int(enc[pos])
	pos++
	if balanceBytes > 0 {
		u.Balance.SetBytes(enc[pos : pos+balanceBytes])
		pos += balanceBytes
	}
	codeHashBytes := int(enc[pos])
	pos++
	if codeHashBytes > 0 {
		copy(u.CodeHashOrStorage[:], enc[pos:pos+codeHashBytes])
	}
}

func (u Update) encode(buf []byte, numBuf []byte) []byte {
	buf = append(buf, byte(u.Flags))
	if u.Flags&BALANCE_UPDATE != 0 {
		buf = append(buf, byte(u.Balance.ByteLen()))
		buf = append(buf, u.Balance.Bytes()...)
	}
	if u.Flags&NONCE_UPDATE != 0 {
		n := binary.PutUvarint(numBuf, u.Nonce)
		buf = append(buf, numBuf[:n]...)
	}
	if u.Flags&CODE_UPDATE != 0 {
		buf = append(buf, u.CodeHashOrStorage[:]...)
	}
	if u.Flags&STORAGE_UPDATE != 0 {
		n := binary.PutUvarint(numBuf, uint64(u.ValLength))
		buf = append(buf, numBuf[:n]...)
		if u.ValLength > 0 {
			buf = append(buf, u.CodeHashOrStorage[:u.ValLength]...)
		}
	}
	return buf
}

func (u *Update) decode(buf []byte, pos int) (int, error) {
	if len(buf) < pos+1 {
		return 0, fmt.Errorf("decode Update: buffer too small for flags")
	}
	u.Flags = UpdateFlags(buf[pos])
	pos++
	if u.Flags&BALANCE_UPDATE != 0 {
		if len(buf) < pos+1 {
			return 0, fmt.Errorf("decode Update: buffer too small for balance len")
		}
		balanceLen := int(buf[pos])
		pos++
		if len(buf) < pos+balanceLen {
			return 0, fmt.Errorf("decode Update: buffer too small for balance")
		}
		u.Balance.SetBytes(buf[pos : pos+balanceLen])
		pos += balanceLen
	}
	if u.Flags&NONCE_UPDATE != 0 {
		var n int
		u.Nonce, n = binary.Uvarint(buf[pos:])
		if n == 0 {
			return 0, fmt.Errorf("decode Update: buffer too small for nonce")
		}
		if n < 0 {
			return 0, fmt.Errorf("decode Update: nonce overflow")
		}
		pos += n
	}
	if u.Flags&CODE_UPDATE != 0 {
		if len(buf) < pos+32 {
			return 0, fmt.Errorf("decode Update: buffer too small for codeHash")
		}
		copy(u.CodeHashOrStorage[:], buf[pos:pos+32])
		pos += 32
	}
	if u.Flags&STORAGE_UPDATE != 0 {
		l, n := binary.Uvarint(buf[pos:])
		if n == 0 {
			return 0, fmt.Errorf("decode Update: buffer too small for storage len")
		}
		if n < 0 {
			return 0, fmt.Errorf("decode Update: storage lee overflow")
		}
		pos += n
		if len(buf) < pos+int(l) {
			return 0, fmt.Errorf("decode Update: buffer too small for storage")
		}
		u.ValLength = int(l)
		copy(u.CodeHashOrStorage[:], buf[pos:pos+int(l)])
		pos += int(l)
	}
	return pos, nil
}

func (u Update) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Flags: [%s]", u.Flags))
	if u.Flags&BALANCE_UPDATE != 0 {
		sb.WriteString(fmt.Sprintf(", Balance: [%d]", &u.Balance))
	}
	if u.Flags&NONCE_UPDATE != 0 {
		sb.WriteString(fmt.Sprintf(", Nonce: [%d]", u.Nonce))
	}
	if u.Flags&CODE_UPDATE != 0 {
		sb.WriteString(fmt.Sprintf(", CodeHash: [%x]", u.CodeHashOrStorage))
	}
	if u.Flags&STORAGE_UPDATE != 0 {
		sb.WriteString(fmt.Sprintf(", Storage: [%x]", u.CodeHashOrStorage[:u.ValLength]))
	}
	return sb.String()
}

// In memory commitment and state to use with the tests
type MockState struct {
	t      *testing.T
	numBuf [binary.MaxVarintLen64]byte
	sm     map[string][]byte     // backbone of the state
	cm     map[string]BranchData // backbone of the commitments
}

func NewMockState(t *testing.T) *MockState {
	t.Helper()
	return &MockState{
		t:  t,
		sm: make(map[string][]byte),
		cm: make(map[string]BranchData),
	}
}

func (ms MockState) branchFn(prefix []byte) ([]byte, error) {
	if exBytes, ok := ms.cm[string(prefix)]; ok {
		return exBytes[2:], nil // Skip touchMap, but keep afterMap
	}
	return nil, nil
}

func (ms MockState) accountFn(plainKey []byte, cell *Cell) error {
	exBytes, ok := ms.sm[string(plainKey)]
	if !ok {
		ms.t.Logf("accountFn not found key [%x]", plainKey)
		return nil
	}
	var ex Update
	pos, err := ex.decode(exBytes, 0)
	if err != nil {
		ms.t.Fatalf("accountFn decode existing [%x], bytes: [%x]: %v", plainKey, exBytes, err)
		return nil
	}
	if pos != len(exBytes) {
		ms.t.Fatalf("accountFn key [%x] leftover bytes in [%x], comsumed %x", plainKey, exBytes, pos)
		return nil
	}
	if ex.Flags&STORAGE_UPDATE != 0 {
		ms.t.Logf("accountFn reading storage item for key [%x]", plainKey)
		return fmt.Errorf("storage read by accountFn")
	}
	if ex.Flags&DELETE_UPDATE != 0 {
		ms.t.Fatalf("accountFn reading deleted account for key [%x]", plainKey)
		return nil
	}
	if ex.Flags&BALANCE_UPDATE != 0 {
		cell.Balance.Set(&ex.Balance)
	} else {
		cell.Balance.Clear()
	}
	if ex.Flags&NONCE_UPDATE != 0 {
		cell.Nonce = ex.Nonce
	} else {
		cell.Nonce = 0
	}
	if ex.Flags&CODE_UPDATE != 0 {
		copy(cell.CodeHash[:], ex.CodeHashOrStorage[:])
	} else {
		copy(cell.CodeHash[:], EmptyCodeHash)
	}
	return nil
}

func (ms MockState) storageFn(plainKey []byte, cell *Cell) error {
	exBytes, ok := ms.sm[string(plainKey)]
	if !ok {
		ms.t.Logf("storageFn not found key [%x]", plainKey)
		return nil
	}
	var ex Update
	pos, err := ex.decode(exBytes, 0)
	if err != nil {
		ms.t.Fatalf("storageFn decode existing [%x], bytes: [%x]: %v", plainKey, exBytes, err)
		return nil
	}
	if pos != len(exBytes) {
		ms.t.Fatalf("storageFn key [%x] leftover bytes in [%x], comsumed %x", plainKey, exBytes, pos)
		return nil
	}
	if ex.Flags&BALANCE_UPDATE != 0 {
		ms.t.Logf("storageFn reading balance for key [%x]", plainKey)
		return nil
	}
	if ex.Flags&NONCE_UPDATE != 0 {
		ms.t.Fatalf("storageFn reading nonce for key [%x]", plainKey)
		return nil
	}
	if ex.Flags&CODE_UPDATE != 0 {
		ms.t.Fatalf("storageFn reading codeHash for key [%x]", plainKey)
		return nil
	}
	if ex.Flags&DELETE_UPDATE != 0 {
		ms.t.Fatalf("storageFn reading deleted item for key [%x]", plainKey)
		return nil
	}
	if ex.Flags&STORAGE_UPDATE != 0 {
		copy(cell.Storage[:], ex.CodeHashOrStorage[:])
		cell.StorageLen = len(ex.CodeHashOrStorage)
	} else {
		cell.Storage = [length.Hash]byte{}
	}
	return nil
}

func (ms *MockState) applyPlainUpdates(plainKeys [][]byte, updates []Update) error {
	for i, key := range plainKeys {
		update := updates[i]
		if update.Flags&DELETE_UPDATE != 0 {
			delete(ms.sm, string(key))
		} else {
			if exBytes, ok := ms.sm[string(key)]; ok {
				var ex Update
				pos, err := ex.decode(exBytes, 0)
				if err != nil {
					return fmt.Errorf("applyPlainUpdates decode existing [%x], bytes: [%x]: %w", key, exBytes, err)
				}
				if pos != len(exBytes) {
					return fmt.Errorf("applyPlainUpdates key [%x] leftover bytes in [%x], comsumed %x", key, exBytes, pos)
				}
				if update.Flags&BALANCE_UPDATE != 0 {
					ex.Flags |= BALANCE_UPDATE
					ex.Balance.Set(&update.Balance)
				}
				if update.Flags&NONCE_UPDATE != 0 {
					ex.Flags |= NONCE_UPDATE
					ex.Nonce = update.Nonce
				}
				if update.Flags&CODE_UPDATE != 0 {
					ex.Flags |= CODE_UPDATE
					copy(ex.CodeHashOrStorage[:], update.CodeHashOrStorage[:])
				}
				if update.Flags&STORAGE_UPDATE != 0 {
					ex.Flags |= STORAGE_UPDATE
					copy(ex.CodeHashOrStorage[:], update.CodeHashOrStorage[:])
				}
				ms.sm[string(key)] = ex.encode(nil, ms.numBuf[:])
			} else {
				ms.sm[string(key)] = update.encode(nil, ms.numBuf[:])
			}
		}
	}
	return nil
}

func (ms *MockState) applyBranchNodeUpdates(updates map[string]BranchData) {
	for key, update := range updates {
		if pre, ok := ms.cm[key]; ok {
			// Merge
			merged, err := pre.MergeHexBranches(update, nil)
			if err != nil {
				panic(err)
			}
			ms.cm[key] = merged
		} else {
			ms.cm[key] = update
		}
	}
}

func decodeHex(in string) []byte {
	payload, err := hex.DecodeString(in)
	if err != nil {
		panic(err)
	}
	return payload
}

// UpdateBuilder collects updates to the state
// and provides them in properly sorted form
type UpdateBuilder struct {
	balances   map[string]*uint256.Int
	nonces     map[string]uint64
	codeHashes map[string][length.Hash]byte
	storages   map[string]map[string][]byte
	deletes    map[string]struct{}
	deletes2   map[string]map[string]struct{}
	keyset     map[string]struct{}
	keyset2    map[string]map[string]struct{}
}

func NewUpdateBuilder() *UpdateBuilder {
	return &UpdateBuilder{
		balances:   make(map[string]*uint256.Int),
		nonces:     make(map[string]uint64),
		codeHashes: make(map[string][length.Hash]byte),
		storages:   make(map[string]map[string][]byte),
		deletes:    make(map[string]struct{}),
		deletes2:   make(map[string]map[string]struct{}),
		keyset:     make(map[string]struct{}),
		keyset2:    make(map[string]map[string]struct{}),
	}
}

func (ub *UpdateBuilder) Balance(addr string, balance uint64) *UpdateBuilder {
	sk := string(decodeHex(addr))
	delete(ub.deletes, sk)
	ub.balances[sk] = uint256.NewInt(balance)
	ub.keyset[sk] = struct{}{}
	return ub
}

func (ub *UpdateBuilder) Nonce(addr string, nonce uint64) *UpdateBuilder {
	sk := string(decodeHex(addr))
	delete(ub.deletes, sk)
	ub.nonces[sk] = nonce
	ub.keyset[sk] = struct{}{}
	return ub
}

func (ub *UpdateBuilder) CodeHash(addr string, hash string) *UpdateBuilder {
	sk := string(decodeHex(addr))
	delete(ub.deletes, sk)
	hcode, err := hex.DecodeString(hash)
	if err != nil {
		panic(fmt.Errorf("invalid code hash provided: %w", err))
	}
	if len(hcode) != length.Hash {
		panic(fmt.Errorf("code hash should be %d bytes long, got %d", length.Hash, len(hcode)))
	}

	dst := [length.Hash]byte{}
	copy(dst[:32], hcode)

	ub.codeHashes[sk] = dst
	ub.keyset[sk] = struct{}{}
	return ub
}

func (ub *UpdateBuilder) Storage(addr string, loc string, value string) *UpdateBuilder {
	sk1 := string(decodeHex(addr))
	sk2 := string(decodeHex(loc))
	v := decodeHex(value)
	if d, ok := ub.deletes2[sk1]; ok {
		delete(d, sk2)
		if len(d) == 0 {
			delete(ub.deletes2, sk1)
		}
	}
	if k, ok := ub.keyset2[sk1]; ok {
		k[sk2] = struct{}{}
	} else {
		ub.keyset2[sk1] = make(map[string]struct{})
		ub.keyset2[sk1][sk2] = struct{}{}
	}
	if s, ok := ub.storages[sk1]; ok {
		s[sk2] = v
	} else {
		ub.storages[sk1] = make(map[string][]byte)
		ub.storages[sk1][sk2] = v
	}
	return ub
}

func (ub *UpdateBuilder) Delete(addr string) *UpdateBuilder {
	sk := string(decodeHex(addr))
	delete(ub.balances, sk)
	delete(ub.nonces, sk)
	delete(ub.codeHashes, sk)
	delete(ub.storages, sk)
	ub.deletes[sk] = struct{}{}
	ub.keyset[sk] = struct{}{}
	return ub
}

func (ub *UpdateBuilder) DeleteStorage(addr string, loc string) *UpdateBuilder {
	sk1 := string(decodeHex(addr))
	sk2 := string(decodeHex(loc))
	if s, ok := ub.storages[sk1]; ok {
		delete(s, sk2)
		if len(s) == 0 {
			delete(ub.storages, sk1)
		}
	}
	if k, ok := ub.keyset2[sk1]; ok {
		k[sk2] = struct{}{}
	} else {
		ub.keyset2[sk1] = make(map[string]struct{})
		ub.keyset2[sk1][sk2] = struct{}{}
	}
	if d, ok := ub.deletes2[sk1]; ok {
		d[sk2] = struct{}{}
	} else {
		ub.deletes2[sk1] = make(map[string]struct{})
		ub.deletes2[sk1][sk2] = struct{}{}
	}
	return ub
}

// Build returns three slices (in the order sorted by the hashed keys)
// 1. Plain keys
// 2. Corresponding hashed keys
// 3. Corresponding updates
func (ub *UpdateBuilder) Build() (plainKeys, hashedKeys [][]byte, updates []Update) {
	var hashed []string
	preimages := make(map[string][]byte)
	preimages2 := make(map[string][]byte)
	keccak := sha3.NewLegacyKeccak256()
	for key := range ub.keyset {
		keccak.Reset()
		keccak.Write([]byte(key))
		h := keccak.Sum(nil)
		hashedKey := make([]byte, len(h)*2)
		for i, c := range h {
			hashedKey[i*2] = (c >> 4) & 0xf
			hashedKey[i*2+1] = c & 0xf
		}
		hashed = append(hashed, string(hashedKey))
		preimages[string(hashedKey)] = []byte(key)
	}
	hashedKey := make([]byte, 128)
	for sk1, k := range ub.keyset2 {
		keccak.Reset()
		keccak.Write([]byte(sk1))
		h := keccak.Sum(nil)
		for i, c := range h {
			hashedKey[i*2] = (c >> 4) & 0xf
			hashedKey[i*2+1] = c & 0xf
		}
		for sk2 := range k {
			keccak.Reset()
			keccak.Write([]byte(sk2))
			h2 := keccak.Sum(nil)
			for i, c := range h2 {
				hashedKey[64+i*2] = (c >> 4) & 0xf
				hashedKey[64+i*2+1] = c & 0xf
			}
			hs := string(common.Copy(hashedKey))
			hashed = append(hashed, hs)
			preimages[hs] = []byte(sk1)
			preimages2[hs] = []byte(sk2)
		}

	}
	slices.Sort(hashed)
	plainKeys = make([][]byte, len(hashed))
	hashedKeys = make([][]byte, len(hashed))
	updates = make([]Update, len(hashed))
	for i, hashedKey := range hashed {
		hashedKeys[i] = []byte(hashedKey)
		key := preimages[hashedKey]
		key2 := preimages2[hashedKey]
		plainKey := make([]byte, len(key)+len(key2))
		copy(plainKey[:], key)
		if key2 != nil {
			copy(plainKey[len(key):], key2)
		}
		plainKeys[i] = plainKey
		u := &updates[i]
		if key2 == nil {
			if balance, ok := ub.balances[string(key)]; ok {
				u.Flags |= BALANCE_UPDATE
				u.Balance.Set(balance)
			}
			if nonce, ok := ub.nonces[string(key)]; ok {
				u.Flags |= NONCE_UPDATE
				u.Nonce = nonce
			}
			if codeHash, ok := ub.codeHashes[string(key)]; ok {
				u.Flags |= CODE_UPDATE
				copy(u.CodeHashOrStorage[:], codeHash[:])
			}
			if _, del := ub.deletes[string(key)]; del {
				u.Flags = DELETE_UPDATE
				continue
			}
		} else {
			if dm, ok1 := ub.deletes2[string(key)]; ok1 {
				if _, ok2 := dm[string(key2)]; ok2 {
					u.Flags = DELETE_UPDATE
					continue
				}
			}
			if sm, ok1 := ub.storages[string(key)]; ok1 {
				if storage, ok2 := sm[string(key2)]; ok2 {
					u.Flags |= STORAGE_UPDATE
					u.CodeHashOrStorage = [length.Hash]byte{}
					u.ValLength = len(storage)
					copy(u.CodeHashOrStorage[:], storage)
				}
			}
		}
	}
	return
}
