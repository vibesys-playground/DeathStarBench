// Package lease fills memcached from MongoDB without ever caching a value
// that a write has already replaced.
//
// The services cache MongoDB documents in memcached (cache-aside). A write
// updates MongoDB, then deletes the key (Invalidate). A plain fill on a miss
// (read MongoDB, then Set) can undo that: a read that loaded the old document
// just before the write stores it just after the delete, and the stale value
// then stays cached indefinitely. A fill therefore takes a lease first:
//
//  1. Acquire adds a unique placeholder under the key (memcached add, which
//     fails if the key holds anything), before the reader queries MongoDB.
//  2. Fill replaces the placeholder with the loaded value by compare-and-swap,
//     which fails if anything changed the key since the placeholder was read.
//
// A write's delete either precedes the reader's add (then the reader's MongoDB
// query starts after the write and loads the new value) or removes the
// placeholder or the filled value (then the fill fails or is discarded). Only
// one reader holds the lease; the others read MongoDB without filling. Readers
// treat a placeholder as a miss (IsLease). Placeholders expire after Expiry so
// a reader that dies does not block caching for long.
package lease

import (
	"bytes"
	"sync/atomic"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/google/uuid"
)

// Expiry is the placeholder's lifetime in seconds.
const Expiry = 10

// prefix starts every placeholder; cached values (JSON documents, decimal
// counts, newline-separated JSON lines) never start with a NUL byte.
var prefix = []byte("\x00lease:")

var (
	instance = uuid.New().String()
	counter  atomic.Uint64
)

// Lease is the right to fill one key.
type Lease struct {
	client *memcache.Client
	key    string
	token  []byte
}

// IsLease reports whether a cached item is a placeholder rather than a value.
func IsLease(item *memcache.Item) bool {
	return item != nil && bytes.HasPrefix(item.Value, prefix)
}

// Acquire takes the lease on key if the key is absent. Call it before reading
// the value from the database; a Lease that was not acquired fills nothing.
func Acquire(client *memcache.Client, key string) Lease {
	token := append(append([]byte(nil), prefix...), instance...)
	token = append(token, ':')
	token = append(token, []byte(hex16(counter.Add(1)))...)
	if err := client.Add(&memcache.Item{Key: key, Value: token, Expiration: Expiry}); err != nil {
		return Lease{}
	}
	return Lease{client: client, key: key, token: token}
}

// Held reports whether the lease was acquired.
func (l Lease) Held() bool { return l.client != nil }

// Fill stores value under the key if the placeholder is still there. It
// reports whether the value was stored.
func (l Lease) Fill(value []byte) bool {
	if l.client == nil {
		return false
	}
	item, err := l.client.Get(l.key)
	if err != nil || !bytes.Equal(item.Value, l.token) {
		return false
	}
	item.Value = value
	item.Expiration = 0
	return l.client.CompareAndSwap(item) == nil
}

// Invalidate deletes key after a write to the value it caches. A key that is
// not cached is not an error.
func Invalidate(client *memcache.Client, key string) error {
	if err := client.Delete(key); err != nil && err != memcache.ErrCacheMiss {
		return err
	}
	return nil
}

func hex16(n uint64) string {
	const digits = "0123456789abcdef"
	var out [16]byte
	for index := len(out) - 1; index >= 0; index-- {
		out[index] = digits[n&15]
		n >>= 4
	}
	return string(out[:])
}
