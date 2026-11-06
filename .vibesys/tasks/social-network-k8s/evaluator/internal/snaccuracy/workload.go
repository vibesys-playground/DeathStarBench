// Package snaccuracy is the social-network-k8s accuracy check's workload on the vibesys-evaluator
// Go SDK: what accuracy.py's run sends, at a fixed rate, for the oracle (internal/snspec) to
// judge. cmd/accuracy-generator runs it (sn-accuracy-generator).
//
// The contract with the oracle: every operation is one snapi.Action, its Info is the action
// (its JSON) and its one step is the action's HTTP request (snapi.Request). Info says what was
// sent, never an expected answer.
//
// The traffic concentrates on the Hot users (the 36 most active users of the dataset and the 4
// most followed), so that writes and reads of the same timelines overlap: they compose (with a
// mention of another hot user 20% of the time, a URL 15%, media 5%), follow and unfollow each
// other, and read their own and each other's timelines. The 4 most followed authors fan out to
// thousands of seeded followers, whose home timelines the reads of the whole dataset (by
// activity and by popularity, as the benchmark draws them) see. A few operations register new
// users and read them, and a few send invalid requests (a non-numeric user, an empty or negative
// window, an unknown user, a post without text).
//
// Markers and registered users are unique per phase (BeginPhase), so no two phases of a run post
// the same marker or register the same user. The model keeps nothing else between operations:
// what a write did is the oracle's to find out from the answers.
package snaccuracy

import (
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/workload"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/snapi"
	"vibesys/social-network-k8s-evaluator/internal/snworkload"
	"vibesys/social-network-k8s-evaluator/internal/timeline"
)

const (
	// ActiveHot and PopularHot are the hot users: the most active users of the dataset, and the
	// most followed ones not among them.
	ActiveHot, PopularHot = 36, 4
	// FirstRegistered is the first id the run registers: phase p registers
	// FirstRegistered + p<<20 + n (below bench.py's probe user, 1999999999).
	FirstRegistered = 1_500_000_000
	maxPhases       = 64
)

// Model is a phase's: the dataset, the hot users, and the markers and users this phase made.
type Model struct {
	data       *dataset.Dataset
	hot        []int32 // user indices
	popular    *dataset.Zipf
	active     *dataset.Zipf
	prefix     string // the run's marker namespace
	nextSeq    int64
	nextUser   int64
	registered []int64 // this phase's registrations, in order
}

// NewModel is the model of a run: rand is its model stream.
func NewModel(data *dataset.Dataset, rand *workload.Rand) (*Model, error) {
	if len(data.Users) < ActiveHot+PopularHot {
		return nil, fmt.Errorf("the dataset has %d users, fewer than the %d hot ones", len(data.Users), ActiveHot+PopularHot)
	}
	return &Model{
		data:    data,
		hot:     Hot(data),
		popular: dataset.NewZipf(len(data.Users), dataset.ZipfOne),
		active:  dataset.NewZipf(len(data.Users), dataset.ZipfThreeQuarters),
		prefix:  fmt.Sprintf("m%010x", rand.Child("markers").Uint64()>>24),
	}, nil
}

// Hot is the hot users' indices: the ActiveHot most active users, then the PopularHot most
// followed users not among them.
func Hot(data *dataset.Dataset) []int32 {
	hot := append([]int32(nil), data.ActivityRank[:ActiveHot]...)
	in := map[int32]bool{}
	for _, u := range hot {
		in[u] = true
	}
	for _, u := range data.PopularityRank {
		if len(hot) == ActiveHot+PopularHot {
			break
		}
		if !in[u] {
			hot = append(hot, u)
		}
	}
	return hot
}

// BeginPhase starts the phase's markers and registered users at its own blocks.
func BeginPhase(m *Model, phase int) error {
	if phase >= maxPhases {
		return fmt.Errorf("phase %d: the accuracy workload has blocks for %d phases", phase, maxPhases)
	}
	m.nextSeq = int64(phase) << 32
	m.nextUser = FirstRegistered + int64(phase)<<20
	m.registered = m.registered[:0]
	return nil
}

// Workload is the accuracy check's workload.
func Workload() workload.Workload[*dataset.Dataset, *Model] {
	return workload.Workload[*dataset.Dataset, *Model]{
		Dataset:    snworkload.LoadDataset,
		Model:      NewModel,
		BeginPhase: BeginPhase,
		Operations: []workload.Operation[*Model]{
			{Name: "compose", Weight: 18, Build: action(buildCompose)},
			{Name: "follow", Weight: 5, Build: action(buildFollow(true))},
			{Name: "unfollow", Weight: 3, Build: action(buildFollow(false))},
			{Name: "home_read", Weight: 30, Build: action(buildHotRead(true))},
			{Name: "user_read", Weight: 20, Build: action(buildHotRead(false))},
			{Name: "home_read_any", Weight: 10, Build: action(buildAnyRead(true))},
			{Name: "user_read_any", Weight: 10, Build: action(buildAnyRead(false))},
			{Name: "register", Weight: 1, Build: action(buildRegister)},
			{Name: "read_registered", Weight: 1, Build: action(buildReadRegistered)},
			{Name: "invalid", Weight: 2, Build: action(buildInvalid)},
		},
	}
}

// action makes an operation of an action builder: the action is the Info, its request the step.
func action(build func(*Model, *workload.Rand) snapi.Action) func(*Model, *workload.Rand) workload.Request {
	return func(m *Model, rand *workload.Rand) workload.Request {
		a := build(m, rand)
		return workload.Request{Steps: []workload.Step{Step(a)}, Info: a}
	}
}

// Step is an action's HTTP request (snapi.Request) as a workload step, its query or form fields
// in the order url.Values.Encode sends them. Every status is an answer for the oracle to judge.
func Step(a snapi.Action) workload.Step {
	method, path, values := snapi.Request(a)
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	options := make([]workload.Option, 0, len(keys))
	for _, k := range keys {
		if method == http.MethodGet {
			options = append(options, workload.Query(k, values.Get(k)))
		} else {
			options = append(options, workload.Form(k, values.Get(k)))
		}
	}
	return workload.HTTP(method, path, options...)
}

func (m *Model) id(user int32) int64 { return m.data.Users[user].ID }

func (m *Model) hotUser(rand *workload.Rand) int32 { return m.hot[rand.Below(len(m.hot))] }

// otherHot is a hot user other than u.
func (m *Model) otherHot(rand *workload.Rand, u int32) int32 {
	for {
		if v := m.hotUser(rand); v != u {
			return v
		}
	}
}

// page is a timeline window start, as the benchmark draws it.
func page(rand *workload.Rand) int { return snworkload.Page(rand.Uint64()) }

var words = strings.Fields(`time day way year week work home city road park river tree garden
	table chair window door book page music song game team ball field water coffee tea bread apple
	lunch market train bus bike plan idea photo color light sound paper box walk run read write cook`)

// buildCompose: a hot author posts a few words and its marker, with a mention of another hot user
// 20% of the time, a URL at the end 15% (the pinned text service drops text after the last
// URL) and media 5%.
func buildCompose(m *Model, rand *workload.Rand) snapi.Action {
	author := m.hotUser(rand)
	marker := m.prefix + strconv.FormatInt(m.nextSeq, 36)
	m.nextSeq++
	count := 2 + rand.Below(6)
	parts := make([]string, 0, count+3)
	for range count {
		parts = append(parts, words[rand.Below(len(words))])
	}
	a := snapi.Action{Kind: snapi.KindCompose, User: m.id(author), Name: m.data.Users[author].Username, Marker: marker}
	if rand.Random() < 0.2 {
		v := m.otherHot(rand, author)
		a.Mentions = []int64{m.id(v)}
		at := rand.Below(len(parts) + 1)
		parts = append(parts[:at], append([]string{"@" + m.data.Users[v].Username}, parts[at:]...)...)
	}
	parts = append(parts, "#"+marker)
	if rand.Random() < 0.15 {
		a.URL = fmt.Sprintf("http://example%d.com", rand.Below(1_000_000))
		parts = append(parts, a.URL)
	}
	a.Text = strings.Join(parts, " ")
	if rand.Random() < 0.05 {
		for i := range 1 + rand.Below(2) {
			id := 1e16 + rand.Uint64()%9e16
			a.Media = append(a.Media, timeline.Media{ID: strconv.FormatUint(id, 10), Type: []string{"png", "jpg"}[i%2]})
		}
	}
	return a
}

// buildFollow: a hot user follows (or unfollows) another.
func buildFollow(on bool) func(*Model, *workload.Rand) snapi.Action {
	return func(m *Model, rand *workload.Rand) snapi.Action {
		u := m.hotUser(rand)
		return snapi.Follow(m.id(u), m.id(m.otherHot(rand, u)), on)
	}
}

// buildHotRead: a hot user's home or user timeline.
func buildHotRead(home bool) func(*Model, *workload.Rand) snapi.Action {
	return func(m *Model, rand *workload.Rand) snapi.Action {
		start := page(rand)
		return snapi.Read(home, m.id(m.hotUser(rand)), start, start+snapi.PageSize)
	}
}

// buildAnyRead: a home timeline of a user drawn by activity, or a user timeline of a user drawn
// by popularity, over the whole dataset (the benchmark's draws).
func buildAnyRead(home bool) func(*Model, *workload.Rand) snapi.Action {
	return func(m *Model, rand *workload.Rand) snapi.Action {
		var user int32
		if home {
			user = m.data.ActivityRank[m.active.Rank(rand.Random())]
		} else {
			user = m.data.PopularityRank[m.popular.Rank(rand.Random())]
		}
		start := page(rand)
		return snapi.Read(home, m.id(user), start, start+snapi.PageSize)
	}
}

// buildRegister registers the phase's next new user.
func buildRegister(m *Model, rand *workload.Rand) snapi.Action {
	id := m.nextUser
	m.nextUser++
	m.registered = append(m.registered, id)
	return snapi.Register(id, "acc"+strconv.FormatInt(id, 10))
}

// buildReadRegistered reads the timeline of a user this phase registered (its registration may
// not be acknowledged yet, or ever), or of the next one if there is none yet.
func buildReadRegistered(m *Model, rand *workload.Rand) snapi.Action {
	id := m.nextUser
	if n := len(m.registered); n > 0 {
		id = m.registered[n-1-rand.Below(min(n, 8))]
	}
	return snapi.Read(rand.Below(2) == 0, id, 0, snapi.PageSize)
}

// buildInvalid: a read with a non-numeric user, an empty or a negative window, or of a user that
// does not exist, or a compose without text.
func buildInvalid(m *Model, rand *workload.Rand) snapi.Action {
	u := m.hotUser(rand)
	id := strconv.FormatInt(m.id(u), 10)
	switch rand.Below(5) {
	case 0:
		return snapi.RawRead(true, "abc", "0", "10")
	case 1:
		return snapi.RawRead(false, id, "5", "5")
	case 2:
		return snapi.RawRead(true, id, "-1", "10")
	case 3:
		return snapi.RawRead(rand.Below(2) == 0, "999999999", "0", "10")
	}
	return snapi.Action{Kind: snapi.KindCompose, User: m.id(u), Name: m.data.Users[u].Username}
}
