package dataset

import (
	"math"
	"sort"
)

// Every random choice uses only integer arithmetic, division and square
// roots, which IEEE 754 defines exactly, so the dataset (and its digest) is
// the same on every platform.

// RNG is SplitMix64. Each kind of entity draws from its own stream (NewRNG),
// so changing how one kind is generated does not reshuffle the others.
type RNG struct{ state uint64 }

// NewRNG returns the stream for one label and index under a seed.
func NewRNG(seed uint64, label string, index int) *RNG {
	hash := uint64(0xcbf29ce484222325) // FNV-1a of the label
	for n := 0; n < len(label); n++ {
		hash = (hash ^ uint64(label[n])) * 0x100000001b3
	}
	return &RNG{state: Mix(seed^hash, uint64(index))}
}

// Next returns 64 random bits.
func (r *RNG) Next() uint64 {
	r.state += 0x9e3779b97f4a7c15
	return finalize(r.state)
}

// Float is uniform in [0, 1).
func (r *RNG) Float() float64 { return float64(r.Next()>>11) / (1 << 53) }

// Intn is uniform in [0, n).
func (r *RNG) Intn(n int) int { return int(r.Next() % uint64(n)) }

// Chance is true with probability p.
func (r *RNG) Chance(p float64) bool { return r.Float() < p }

// Pareto2 is a shifted Pareto variable with tail exponent 2, an integer
// floor(scale * (1/sqrt(U) - 1)) >= 0: median 0.41 scale, mean scale,
// P(X > x) = (scale / (x + scale))^2.
func (r *RNG) Pareto2(scale float64) int {
	u := 1 - r.Float() // (0, 1]
	return int(scale * (1/math.Sqrt(u) - 1))
}

func finalize(value uint64) uint64 {
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

// Mix derives an independent 64-bit value from a random word and a stream
// number. Request generators draw every choice of one sample from
// Mix(sample, stream) with a distinct stream per choice.
func Mix(random, stream uint64) uint64 {
	return finalize(random + 0x9e3779b97f4a7c15*(stream+1))
}

// Unit maps Mix(random, stream) to a uniform float in [0, 1).
func Unit(random, stream uint64) float64 {
	return float64(Mix(random, stream)>>11) / (1 << 53)
}

// ZipfExponent selects a Zipf exponent whose weights rank^-s need square
// roots only.
type ZipfExponent int

const (
	ZipfHalf          ZipfExponent = iota // s = 0.5
	ZipfThreeQuarters                     // s = 0.75
	ZipfOne                               // s = 1
)

func zipfWeight(rank int, exponent ZipfExponent) float64 {
	k := float64(rank + 1)
	switch exponent {
	case ZipfHalf:
		return 1 / math.Sqrt(k)
	case ZipfThreeQuarters:
		return 1 / (math.Sqrt(k) * math.Sqrt(math.Sqrt(k)))
	default:
		return 1 / k
	}
}

// Zipf samples ranks 0..n-1 with probability proportional to (rank+1)^-s by
// inverse CDF.
type Zipf struct{ cumulative []float64 }

// NewZipf returns a sampler over n ranks.
func NewZipf(n int, exponent ZipfExponent) *Zipf {
	cumulative := make([]float64, n)
	sum := 0.0
	for rank := range cumulative {
		sum += zipfWeight(rank, exponent)
		cumulative[rank] = sum
	}
	return &Zipf{cumulative: cumulative}
}

// Len is the number of ranks.
func (z *Zipf) Len() int { return len(z.cumulative) }

// Rank maps a uniform u in [0, 1) to a rank.
func (z *Zipf) Rank(u float64) int {
	index := sort.SearchFloat64s(z.cumulative, u*z.cumulative[len(z.cumulative)-1])
	if index >= len(z.cumulative) {
		index = len(z.cumulative) - 1
	}
	return index
}

// Probability is the probability of one rank.
func (z *Zipf) Probability(rank int) float64 {
	previous := 0.0
	if rank > 0 {
		previous = z.cumulative[rank-1]
	}
	return (z.cumulative[rank] - previous) / z.cumulative[len(z.cumulative)-1]
}
