package snapshot

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"

	"go.mongodb.org/mongo-driver/bson"

	"vibesys/hotel-k8s-evaluator/internal/dataset"
)

// versionSamples is how many hotels and users Version encodes besides the first and the last.
const versionSamples = 61

// Version names the snapshot of a dataset: a digest of the layout, the MongoDB image, the
// spec, every collection's name, indexes and document count, and the BSON of every document
// of a spread sample of hotels and users. Any change of what the generator writes for these
// (and so, in practice, for any hotel: every hotel comes from the same code) or of the
// collections changes it, so a snapshot is never reused for other data; an unchanged
// generator keeps it, so the snapshot is built once.
func Version(d *dataset.Dataset) string {
	h := sha256.New()
	spec, _ := json.Marshal(d.Spec)
	fmt.Fprintf(h, "hotel-k8s-snapshot layout %d\x00%s\x00%s\x00", Layout, MongoImage, spec)
	for _, c := range Collections() {
		fmt.Fprintf(h, "%s.%s\x00%d\x00", c.Database, c.Name, c.Count(d))
		for _, index := range c.Indexes {
			raw, _ := bson.Marshal(index.Keys)
			fmt.Fprintf(h, "%s %v %v %x\x00", index.Name, index.Unique, index.Sparse, raw)
		}
		for _, sample := range samples(c, d) {
			raw, err := bson.Marshal(sample)
			if err != nil {
				panic(err)
			}
			var size [4]byte
			binary.LittleEndian.PutUint32(size[:], uint32(len(raw)))
			h.Write(size[:])
			h.Write(raw)
		}
	}
	return hex.EncodeToString(h.Sum(nil))[:20]
}

// samples is the documents of the collection's sampled hotels or users.
func samples(c Collection, d *dataset.Dataset) []any {
	var out []any
	switch {
	case c.PerHotel != nil:
		for _, i := range spread(d.HotelCount()) {
			out = c.PerHotel(d, i, out)
		}
	case c.PerUser != nil:
		for _, u := range spread(len(d.Users)) {
			out = c.PerUser(d, u, out)
		}
	}
	return out
}

// spread is 0, n-1 and versionSamples indices evenly between.
func spread(n int) []int {
	out := []int{0}
	for k := 1; k <= versionSamples; k++ {
		if index := k * n / (versionSamples + 1); index > out[len(out)-1] && index < n-1 {
			out = append(out, index)
		}
	}
	if n > 1 {
		out = append(out, n-1)
	}
	return out
}

// parallel runs body(0..n-1) on every core; body must touch only its own index's state.
func parallel(n int, body func(int)) {
	workers := runtime.GOMAXPROCS(0)
	chunk := (n + workers - 1) / workers
	var wg sync.WaitGroup
	for start := 0; start < n; start += max(chunk, 1) {
		end := min(start+chunk, n)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := start; index < end; index++ {
				body(index)
			}
		}()
	}
	wg.Wait()
}
