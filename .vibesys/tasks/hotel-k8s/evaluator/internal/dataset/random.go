package dataset

import (
	"math"
	"sort"
)

// rng is SplitMix64. Each entity has its own stream (newRNG), so adding a
// field to one kind of record does not reshuffle the others.
type rng struct{ state uint64 }

func newRNG(seed uint64, label string, index int) *rng {
	// FNV-1a of the label, mixed with the seed and the index.
	hash := uint64(0xcbf29ce484222325)
	for n := 0; n < len(label); n++ {
		hash = (hash ^ uint64(label[n])) * 0x100000001b3
	}
	return &rng{state: Mix(seed^hash, uint64(index))}
}

func (r *rng) next() uint64 {
	r.state += 0x9e3779b97f4a7c15
	return finalize(r.state)
}

// float is uniform in [0, 1).
func (r *rng) float() float64 { return float64(r.next()>>11) / (1 << 53) }

func (r *rng) intn(n int) int { return int(r.next() % uint64(n)) }

// normal is approximately standard normal: the centred sum of four uniforms,
// scaled to unit variance and bounded at 2 sqrt(3).
func (r *rng) normal() float64 {
	return (r.float() + r.float() + r.float() + r.float() - 2) * math.Sqrt(3)
}

// pick returns an index with probability proportional to its weight.
func (r *rng) pick(weights []int) int {
	total := 0
	for _, weight := range weights {
		total += weight
	}
	draw := r.intn(total)
	for index, weight := range weights {
		if draw < weight {
			return index
		}
		draw -= weight
	}
	return len(weights) - 1
}

func shuffle(r *rng, n int, swap func(i, j int)) {
	for i := n - 1; i > 0; i-- {
		swap(i, r.intn(i+1))
	}
}

func finalize(value uint64) uint64 {
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

// Mix derives an independent 64-bit value from a random word and a stream
// number (SplitMix64 finalizer). Request generators draw every choice of one
// sample from Mix(sample, stream) with a distinct stream per choice.
func Mix(random, stream uint64) uint64 {
	return finalize(random + 0x9e3779b97f4a7c15*(stream+1))
}

// Unit maps Mix(random, stream) to a uniform float in [0, 1).
func Unit(random, stream uint64) float64 {
	return float64(Mix(random, stream)>>11) / (1 << 53)
}

// ZipfExponent is a Zipf exponent whose weights rank^-s are computed with
// square roots only, so they are bit-identical on every platform.
type ZipfExponent int

const (
	ZipfHalf          ZipfExponent = iota // s = 0.5
	ZipfThreeQuarters                     // s = 0.75
	ZipfOne                               // s = 1
)

// ZipfWeights returns the normalized weights rank^-s of ranks 1..n.
func ZipfWeights(n int, exponent ZipfExponent) []float64 {
	weights := make([]float64, n)
	total := 0.0
	for index := range weights {
		k := float64(index + 1)
		switch exponent {
		case ZipfHalf:
			weights[index] = 1 / math.Sqrt(k)
		case ZipfThreeQuarters:
			weights[index] = 1 / (math.Sqrt(k) * math.Sqrt(math.Sqrt(k)))
		default:
			weights[index] = 1 / k
		}
		total += weights[index]
	}
	for index := range weights {
		weights[index] /= total
	}
	return weights
}

// Zipf samples ranks 0..n-1 with Zipf weights by inverse CDF.
type Zipf struct{ cumulative []float64 }

// NewZipf returns a sampler over n ranks.
func NewZipf(n int, exponent ZipfExponent) *Zipf {
	weights := ZipfWeights(n, exponent)
	cumulative := make([]float64, n)
	sum := 0.0
	for index, weight := range weights {
		sum += weight
		cumulative[index] = sum
	}
	return &Zipf{cumulative: cumulative}
}

// Rank maps a uniform u in [0, 1) to a rank.
func (z *Zipf) Rank(u float64) int {
	index := sort.SearchFloat64s(z.cumulative, u*z.cumulative[len(z.cumulative)-1])
	if index >= len(z.cumulative) {
		index = len(z.cumulative) - 1
	}
	return index
}

// Normal maps the four Mix streams stream..stream+3 to an approximately standard
// normal value, like the generator's hotel offsets (Irwin-Hall of four uniforms).
func Normal(random, stream uint64) float64 {
	return (Unit(random, stream) + Unit(random, stream+1) + Unit(random, stream+2) + Unit(random, stream+3) - 2) * math.Sqrt(3)
}
