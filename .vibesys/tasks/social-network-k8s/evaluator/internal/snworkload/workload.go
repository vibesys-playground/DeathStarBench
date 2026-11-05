// Package snworkload is the social-network-k8s benchmark's workload on the vibesys-evaluator Go
// SDK: the operations of the mix with their weights, how each request is drawn, how a write
// changes the model, and how each response is checked after the phase. cmd/generator runs it as
// the generator that loadgen reads the request stream from (bench.py names it).
//
// The model is the accuracy check's: internal/oracle, the seeded dataset plus the writes of the
// sample (every earlier phase's, replayed from the history, and this phase's) with their times.
// A response is checked by the oracle's Step, so the benchmark and the check enforce one
// contract (OBJECTIVE.md, "Correctness bar").
//
// Operations (weights 500/400/100/5/5), each drawn from one 64-bit word of the operation's own
// stream, as the earlier servicebench adapter drew them from its sample word:
//
//   - user_timeline_read: the user timeline of a user drawn by Zipf (s = 1) over follower
//     count (profiles of popular users are viewed most);
//   - home_timeline_read: the home timeline of a user drawn by Zipf (s = 0.75) over post
//     count (active users read and post most);
//   - compose_post: an active user posts (a few words from a short list, a unique "#m..."
//     marker, a mention of a followee 20% of the time, a URL at the end 15%, media 5%), then
//     reads their own user timeline, which must hold the post (read your write);
//   - follow / unfollow: an active user follows a popular user, or drops a followee; changes of
//     one edge never overlap (the request's exclusive key).
//
// Pages are 10 posts: the first 80%, the second 12%, the third 5%, pages 4-7 3%.
package snworkload

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/loadgen/workload"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/oracle"
	"vibesys/social-network-k8s-evaluator/internal/timeline"
)

// Bound is B of OBJECTIVE.md's correctness bar.
const Bound = time.Second

const (
	opUserTimeline = "user_timeline_read"
	opHomeTimeline = "home_timeline_read"
	opCompose      = "compose_post"
	opFollow       = "follow"
	opUnfollow     = "unfollow"
	pageSize       = oracle.PageSize
)

// Streams of dataset.Mix(word, stream) for the choices of one operation (the earlier adapter's).
const (
	streamUser = iota
	streamPage
	streamOther
	streamRetry = 1000
)

// Draw streams of one compose (dataset.Mix(word, stream)).
const (
	streamWords = 100 + iota
	streamMention
	streamURL
	streamMedia
)

// DatasetFile is the dataset file bench.py's dataset() writes: the seed and digest of the
// task's generated dataset, which every process regenerates (internal/dataset).
type DatasetFile struct {
	Seed   uint64 `json:"seed"`
	Digest string `json:"digest"`
}

// LoadDataset generates the dataset a dataset file names and checks its digest.
func LoadDataset(path string) (*dataset.Dataset, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var file DatasetFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	data := dataset.Default()
	if file.Seed != dataset.DefaultSeed {
		data = dataset.Generate(file.Seed, dataset.DefaultConfig())
	}
	if got := data.Digest(); got != file.Digest {
		return nil, fmt.Errorf("the dataset of seed %d has digest %s, the dataset file says %s (dataset.py and internal/dataset disagree)", file.Seed, got, file.Digest)
	}
	return data, nil
}

// Model is the oracle's model and state of a sample, with what generation draws from.
type Model struct {
	data    *dataset.Dataset
	oracle  *oracle.Model
	state   *oracle.State
	popular *dataset.Zipf // user-timeline targets, by PopularityRank
	active  *dataset.Zipf // readers, authors, followers, by ActivityRank
	prefix  string        // this sample's marker namespace
	nextSeq int64
}

// NewModel is the model of a sample: rand is the sample's model stream.
func NewModel(data *dataset.Dataset, rand *workload.Rand) (*Model, error) {
	return &Model{
		data: data, oracle: oracle.NewModel(data, Bound), state: oracle.NewState(),
		popular: dataset.NewZipf(len(data.Users), dataset.ZipfOne),
		active:  dataset.NewZipf(len(data.Users), dataset.ZipfThreeQuarters),
		prefix:  fmt.Sprintf("m%010x", rand.Child("markers").Uint64()>>24),
	}, nil
}

// State is the oracle state the model holds (tests).
func (m *Model) State() *oracle.State { return m.state }

// Workload is the benchmark's workload.
func Workload() workload.Workload[*dataset.Dataset, *Model] {
	return workload.Workload[*dataset.Dataset, *Model]{
		Dataset: LoadDataset,
		Model:   NewModel,
		Operations: []workload.Operation[*Model]{
			{Name: opUserTimeline, Weight: 500, Build: buildUserTimeline, Check: checkRead(false)},
			{Name: opHomeTimeline, Weight: 400, Build: buildHomeTimeline, Check: checkRead(true)},
			{Name: opCompose, Weight: 100, Build: buildCompose, Check: checkCompose, Apply: applyCompose},
			{Name: opFollow, Weight: 5, Build: buildEdge(true), Check: checkEdge, Apply: applyEdge},
			{Name: opUnfollow, Weight: 5, Build: buildEdge(false), Check: checkEdge, Apply: applyEdge},
		},
	}
}

// ---------------------------------------------------------------- request distributions

// Page draws a timeline page start: the first page 80% of the time, the second 12%, the third
// 5%, pages 4-7 3%.
func Page(word uint64) int {
	u := dataset.Unit(word, streamPage)
	switch {
	case u < 0.80:
		return 0
	case u < 0.92:
		return pageSize
	case u < 0.97:
		return 2 * pageSize
	default:
		return pageSize * (3 + int(dataset.Mix(word, streamPage+50)%4))
	}
}

func (m *Model) activeUser(word, stream uint64) int32 {
	return m.data.ActivityRank[m.active.Rank(dataset.Unit(word, stream))]
}

func (m *Model) popularUser(word, stream uint64) int32 {
	return m.data.PopularityRank[m.popular.Rank(dataset.Unit(word, stream))]
}

func (m *Model) id(user int32) int64 { return m.data.Users[user].ID }

// readInfo is a read's Info: the reader (user index) and the page start.
type readInfo struct {
	User  int32 `json:"user"`
	Start int   `json:"start"`
}

func timelineStep(home bool, id int64, start int) workload.Step {
	path := "/wrk2-api/user-timeline/read"
	if home {
		path = "/wrk2-api/home-timeline/read"
	}
	return workload.Get(path,
		workload.Query("user_id", strconv.FormatInt(id, 10)),
		workload.Query("start", strconv.Itoa(start)),
		workload.Query("stop", strconv.Itoa(start+pageSize)))
}

func buildUserTimeline(m *Model, rand *workload.Rand) workload.Request {
	word := rand.Uint64()
	user, start := m.popularUser(word, streamUser), Page(word)
	return workload.Request{Steps: []workload.Step{timelineStep(false, m.id(user), start)}, Info: readInfo{user, start}}
}

func buildHomeTimeline(m *Model, rand *workload.Rand) workload.Request {
	word := rand.Uint64()
	user, start := m.activeUser(word, streamUser), Page(word)
	return workload.Request{Steps: []workload.Step{timelineStep(true, m.id(user), start)}, Info: readInfo{user, start}}
}

// composeInfo is a compose's Info: the author (user index), its sequence number in the sample
// and the word its post is drawn from (decimal, as JSON numbers lose bits in other readers).
type composeInfo struct {
	Author int32  `json:"author"`
	Seq    int64  `json:"seq"`
	Word   string `json:"word"`
}

var composeWords = strings.Fields(`time day way year week work home city road park river tree garden
	table chair window door book page music song game team ball field water coffee tea bread apple
	lunch market train bus bike plan idea photo color light sound paper box walk run read write cook
	paint build open start finish visit share watch listen learn help find good new big small long
	early late quiet happy busy warm cold green blue bright fresh simple today tomorrow again soon`)

// compose is the post of author drawn from word: a few words, a unique marker token, a mention
// of a followee, a URL at the end (the pinned text service drops text after the last URL),
// media.
func (m *Model) compose(author int32, word uint64, seq int64) oracle.Action {
	data := m.data
	marker := m.prefix + strconv.FormatInt(seq, 36)
	rng := dataset.NewRNG(word, "compose", 0)
	count := min(2+rng.Pareto2(5), 25)
	parts := make([]string, 0, count+4)
	for i := 0; i < count; i++ {
		parts = append(parts, composeWords[rng.Intn(len(composeWords))])
	}
	a := oracle.Action{Kind: oracle.KindCompose, User: m.id(author), Name: data.Users[author].Username, Marker: marker}
	followees := data.Followees.Row(int(author))
	if len(followees) > 0 && dataset.Unit(word, streamMention) < 0.2 {
		v := followees[dataset.Mix(word, streamMention+50)%uint64(len(followees))]
		a.Mentions = []int64{m.id(v)}
		at := int(dataset.Mix(word, streamMention+51) % uint64(len(parts)+1))
		parts = append(parts[:at], append([]string{"@" + data.Users[v].Username}, parts[at:]...)...)
	}
	parts = append(parts, "#"+marker)
	if dataset.Unit(word, streamURL) < 0.15 {
		a.URL = fmt.Sprintf("http://example%d.com", dataset.Mix(word, streamURL+50)%1000000)
		parts = append(parts, a.URL)
	}
	a.Text = strings.Join(parts, " ")
	if dataset.Unit(word, streamMedia) < 0.05 {
		items := 1 + int(dataset.Mix(word, streamMedia+50)%2)
		for i := 0; i < items; i++ {
			id := 1e16 + dataset.Mix(word, streamMedia+51+uint64(i))%9e16
			a.Media = append(a.Media, timeline.Media{ID: strconv.FormatUint(id, 10), Type: "png"})
		}
	}
	return a
}

func buildCompose(m *Model, rand *workload.Rand) workload.Request {
	word := rand.Uint64()
	author := m.activeUser(word, streamUser)
	seq := m.nextSeq
	a := m.compose(author, word, seq)
	_, _, form := oracle.Request(a)
	return workload.Request{
		Steps: []workload.Step{
			// The form's fields in the order url.Values.Encode sends them.
			workload.Post("/wrk2-api/post/compose",
				workload.Form("media_ids", form.Get("media_ids")), workload.Form("media_types", form.Get("media_types")),
				workload.Form("post_type", form.Get("post_type")), workload.Form("text", form.Get("text")),
				workload.Form("user_id", form.Get("user_id")), workload.Form("username", form.Get("username"))),
			timelineStep(false, a.User, 0),
		},
		Info: composeInfo{Author: author, Seq: seq, Word: strconv.FormatUint(word, 10)},
	}
}

// edgeInfo is a follow change's Info: follower and followee (user indices), and the new state.
type edgeInfo struct {
	U  int32 `json:"u"`
	V  int32 `json:"v"`
	On bool  `json:"on"`
}

// buildEdge draws a follow of a popular user not yet followed, or an unfollow of a current
// followee, by an active user; the request holds the edge's exclusive key.
func buildEdge(follow bool) func(*Model, *workload.Rand) workload.Request {
	return func(m *Model, rand *workload.Rand) workload.Request {
		word := rand.Uint64()
		u, v := m.pickEdge(follow, word)
		path := "/wrk2-api/user/unfollow"
		if follow {
			path = "/wrk2-api/user/follow"
		}
		return workload.Request{
			Steps: []workload.Step{workload.Post(path,
				workload.Form("followee_id", strconv.FormatInt(m.id(v), 10)),
				workload.Form("user_id", strconv.FormatInt(m.id(u), 10)))},
			Info:      edgeInfo{U: u, V: v, On: follow},
			Exclusive: fmt.Sprintf("edge:%d:%d", u, v),
		}
	}
}

func (m *Model) follows(u, v int32) bool { return m.oracle.Follows(m.state, m.id(u), m.id(v)) }

// pickEdge is the earlier adapter's draw: 64 attempts, then a scan for any eligible edge.
func (m *Model) pickEdge(follow bool, word uint64) (u, v int32) {
	n := len(m.data.Users)
	for attempt := uint64(0); attempt < 64; attempt++ {
		stream := streamRetry + 3*attempt
		candidate := m.activeUser(word, stream)
		var target int32
		if follow {
			target = m.popularUser(word, stream+1)
		} else {
			row := m.data.Followees.Row(int(candidate))
			if len(row) == 0 {
				continue
			}
			target = row[dataset.Mix(word, stream+1)%uint64(len(row))]
		}
		if target != candidate && m.follows(candidate, target) != follow {
			return candidate, target
		}
	}
	for scan := 0; scan < n*4; scan++ {
		candidate := int32((int(word%uint64(n)) + scan) % n)
		row := m.data.Followees.Row(int(candidate))
		if len(row) == 0 {
			continue
		}
		target := row[scan%len(row)]
		if !follow {
			if m.follows(candidate, target) {
				return candidate, target
			}
		} else if other := int32((int(target) + 1) % n); other != candidate && !m.follows(candidate, other) {
			return candidate, other
		}
	}
	panic(fmt.Sprintf("no edge to %s", map[bool]string{true: "follow", false: "unfollow"}[follow]))
}

// ---------------------------------------------------------------- writes -> model

// step is the oracle call of one step of a call: its answer, over the operation's times.
func step(call workload.Call, index int, action oracle.Action) oracle.Call {
	c := oracle.Call{Action: action, Start: call.Start, End: call.End, Pending: call.Pending}
	if index < len(call.Responses) {
		c.Status, c.Body = call.Responses[index].Status, call.Responses[index].Body
	}
	if !call.Pending && c.Status == 0 {
		c.Err = "no response"
	}
	return c
}

func (m *Model) composeOf(call workload.Call) (oracle.Action, composeInfo, error) {
	var info composeInfo
	if err := call.DecodeInfo(&info); err != nil {
		return oracle.Action{}, info, err
	}
	word, err := strconv.ParseUint(info.Word, 10, 64)
	if err != nil {
		return oracle.Action{}, info, fmt.Errorf("compose word %q: %w", info.Word, err)
	}
	if info.Author < 0 || int(info.Author) >= len(m.data.Users) {
		return oracle.Action{}, info, fmt.Errorf("compose author %d is not a seeded user", info.Author)
	}
	return m.compose(info.Author, word, info.Seq), info, nil
}

// applyCompose records the post: acknowledged when the compose step's answer acknowledged it,
// at the operation's end at the latest (the oracle's times only ever favour the application).
func applyCompose(m *Model, call workload.Call) error {
	a, info, err := m.composeOf(call)
	if err != nil {
		return err
	}
	m.oracle.Record(m.state, step(call, 0, a))
	m.nextSeq = max(m.nextSeq, info.Seq+1)
	return nil
}

func (m *Model) edgeOf(call workload.Call) (oracle.Action, error) {
	var info edgeInfo
	if err := call.DecodeInfo(&info); err != nil {
		return oracle.Action{}, err
	}
	n := int32(len(m.data.Users))
	if info.U < 0 || info.U >= n || info.V < 0 || info.V >= n {
		return oracle.Action{}, fmt.Errorf("edge %d -> %d is not between seeded users", info.U, info.V)
	}
	return oracle.Follow(m.id(info.U), m.id(info.V), info.On), nil
}

func applyEdge(m *Model, call workload.Call) error {
	a, err := m.edgeOf(call)
	if err != nil {
		return err
	}
	m.oracle.Record(m.state, step(call, 0, a))
	return nil
}

// ---------------------------------------------------------------- checks

func checkRead(home bool) func(*Model, workload.Call) error {
	return func(m *Model, call workload.Call) error {
		var info readInfo
		if err := call.DecodeInfo(&info); err != nil {
			return err
		}
		if info.User < 0 || int(info.User) >= len(m.data.Users) {
			return fmt.Errorf("reader %d is not a seeded user", info.User)
		}
		read := oracle.Read(home, m.id(info.User), info.Start, info.Start+pageSize)
		return m.oracle.Apply(m.state, step(call, 0, read))
	}
}

// checkCompose: the compose was acknowledged, and the author's user timeline read right after
// it holds the post (read your write), with every other post as the oracle places it.
func checkCompose(m *Model, call workload.Call) error {
	a, _, err := m.composeOf(call)
	if err != nil {
		return err
	}
	if err := m.oracle.Answer(m.state, step(call, 0, a)); err != nil {
		return err
	}
	read := oracle.Read(false, a.User, 0, pageSize)
	read.Holds = []string{a.Marker}
	if err := m.oracle.Apply(m.state, step(call, 1, read)); err != nil {
		return fmt.Errorf("read your write: %w", err)
	}
	return nil
}

func checkEdge(m *Model, call workload.Call) error {
	a, err := m.edgeOf(call)
	if err != nil {
		return err
	}
	return m.oracle.Answer(m.state, step(call, 0, a))
}
