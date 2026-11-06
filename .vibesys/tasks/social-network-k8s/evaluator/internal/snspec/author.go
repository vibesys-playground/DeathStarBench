package snspec

import (
	"fmt"
	"iter"
	"strconv"
	"strings"
	"time"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/check"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/snapi"
	"vibesys/social-network-k8s-evaluator/internal/timeline"
)

// Bound is B of OBJECTIVE.md: home timelines and follow changes are visible to reads that start
// more than this long after the write was acknowledged.
const Bound = time.Second

// EffectWindow is how long after its call an unknown outcome may still take effect: loadgen's
// default step timeout.
const EffectWindow = 10 * time.Second

// Slack bounds the difference between the application's clock and the client's when a live
// post's timestamp is checked against the time it was composed.
const Slack = 5 * time.Second

type (
	piece      = check.Piece[any, any]
	projection = check.Projection[any, any]
)

func ukey(id int64) string { return "u:" + strconv.FormatInt(id, 10) }

// AuthorSpec is the per-author view (see the package comment) over the seeded dataset d (nil:
// none), with the bound and the clock slack of live post timestamps.
func AuthorSpec(d *dataset.Dataset, bound, slack time.Duration) *check.Spec[State, any, any] {
	m := NewModel(d, int64(bound), int64(slack))
	return &check.Spec[State, any, any]{
		Name:    "social-network/author",
		Project: m.projectAuthor,
		Init:    func([]string) State { return NewState() },
		Step:    m.step,
		// Fault nil: restarts and flushes change nothing in the contract (every acknowledged
		// write survives), so they are not replicated into partitions.
		Equal:         func(a, b State) bool { return a.Equal(b) },
		Hash:          func(s State) uint64 { return s.Hash() },
		EffectWindow:  EffectWindow,
		DescribeState: func(s State) string { return s.Describe() },
		DescribeOp:    describeOp,
	}
}

// DefaultAuthorSpec and DefaultReaderSpec use the task's bound and slack.
func DefaultAuthorSpec(d *dataset.Dataset) *check.Spec[State, any, any] {
	return AuthorSpec(d, Bound, Slack)
}

func (m *Model) seededUserByName(name string) int {
	if m.Data == nil || !strings.HasPrefix(name, "user") {
		return -1
	}
	id, err := strconv.ParseInt(name[4:], 10, 64)
	if err != nil {
		return -1
	}
	return m.seededUser(id)
}

// projectAuthor is the per-author spec's Project. Whatever needs the whole history (who is
// known, who follows whom, who mentions whom) is collected in a first pass over the phases; the
// pieces are added in a second.
func (m *Model) projectAuthor(phases iter.Seq[[]check.Call], out *projection) error {
	h, err := m.collect(phases)
	if err != nil {
		return err
	}
	d := m.Data
	nameOf := func(id int64) string {
		if n, ok := h.names[id]; ok {
			return n
		}
		if i := m.seededUser(id); i >= 0 {
			return d.Users[i].Username
		}
		return ""
	}
	knownEver := func(id int64) bool { return h.known[id] || m.seededUser(id) >= 0 }
	return calls(phases, func(ac *acall) {
		a := ac.a
		add := func(key string, in, o any, write bool) {
			out.Add(piece{Keys: []string{key}, In: in, Out: o, Write: write, Unknown: ac.unknown, Origin: ac.origin})
		}
		wo := WriteOut{Status: ac.status, Acked: ac.acked}
		switch a.Kind {
		case snapi.KindRegister:
			add(ukey(a.User), Register{ID: a.User}, wo, true)
			add("n:"+a.Name, Claim{Name: a.Name, Taken: m.seededUserByName(a.Name) >= 0}, wo, true)
		case snapi.KindCompose:
			cp := Compose{Author: a.User, AuthorName: nameOf(a.User), Marker: a.Marker, Text: a.Text, URL: a.URL, Mentions: a.Mentions, Media: a.Media, Empty: a.Text == ""}
			for _, v := range a.Mentions {
				cp.MentionNames = append(cp.MentionNames, nameOf(v))
			}
			add(ukey(a.User), cp, wo, true)
		case snapi.KindFollow:
			add(ukey(a.Followee), Follow{U: a.User, V: a.Followee, On: a.On}, wo, true)
		case snapi.KindRead:
			m.projectRead(out, ac, knownEver, h)
		}
	})
}

func (m *Model) projectRead(out *projection, ac *acall, knownEver func(int64) bool, h *facts) {
	a := ac.a
	d := m.Data
	user, errU := strconv.ParseInt(a.UserID, 10, 64)
	_, start, stop, valid := pageWindow(a)
	key := "x:" + a.UserID
	if errU == nil {
		key = ukey(user)
	}
	ro := ac.ro
	add := func(k string, in, o any) {
		out.Add(piece{Keys: []string{k}, In: in, Out: o, Unknown: ac.unknown, Origin: ac.origin})
	}
	want := stop - start
	info := pageInfo(ro.Page)
	cutoff := int64(-1 << 62)
	if valid && info.Len == want {
		cutoff = info.Lo
	}
	seededIdx := -1
	if errU == nil {
		seededIdx = m.seededUser(user)
	}
	if !a.Home {
		ur := UserRead{User: user, Valid: valid, Start: start, End: stop, Holds: a.Holds}
		if valid && seededIdx >= 0 {
			for _, p := range d.UserPosts.Row(seededIdx) { // newest first
				ts := d.Posts[p].Timestamp
				if ts < cutoff {
					break
				}
				ur.Seeded = append(ur.Seeded, SeedCand{Idx: p, TS: ts})
			}
		}
		add(key, ur, ro)
		return
	}
	hc := HomeCheck{Reader: user, Valid: valid, Start: start, End: stop}
	if !valid || ac.unknown || ro.Status != 200 || ro.ParseErr != "" || !knownEver(user) {
		add(key, hc, ro)
		return
	}
	set := map[int64]bool{}
	for i := range ro.Page {
		if id, err := strconv.ParseInt(ro.Page[i].CreatorID, 10, 64); err == nil {
			set[id] = true
		}
	}
	for v := range h.changedBy[user] {
		set[v] = true
	}
	for v := range h.mentioners[user] {
		set[v] = true
	}
	var row []int32
	if seededIdx >= 0 {
		for _, f := range d.Followees.Row(seededIdx) {
			if id := d.Users[f].ID; h.liveAuthors[id] {
				set[id] = true
			}
		}
		row = d.Home.Row(seededIdx)
	}
	seeded := map[int64][]SeedCand{}
	for _, p := range row {
		post := &d.Posts[p]
		if post.Timestamp < cutoff {
			break
		}
		au := d.Users[post.Author].ID
		sc := SeedCand{Idx: p, TS: post.Timestamp}
		for _, mn := range post.Mentions {
			if int(mn) == seededIdx {
				sc.Mentioned = true
			}
		}
		if set[au] {
			seeded[au] = append(seeded[au], sc)
		} else {
			hc.Static = append(hc.Static, sc)
		}
	}
	add(key, hc, ro)
	byAuthor := map[int64][]timeline.Post{}
	for i := range ro.Page {
		id, err := strconv.ParseInt(ro.Page[i].CreatorID, 10, 64)
		if err != nil {
			id = -1
		}
		byAuthor[id] = append(byAuthor[id], ro.Page[i])
		set[id] = true
	}
	for au := range set {
		hp := HomePart{Reader: user, Author: au, Start: start, End: stop, Holds: a.Holds, Seeded: seeded[au], Info: info, SeededFollow: m.seededFollows(user, au)}
		if ia := m.seededUser(au); ia >= 0 && h.changedBy[user][au] && !hp.SeededFollow {
			for _, p := range d.UserPosts.Row(ia) {
				post := &d.Posts[p]
				if post.Timestamp < cutoff {
					break
				}
				hp.Extra = append(hp.Extra, SeedCand{Idx: p, TS: post.Timestamp})
			}
		}
		k := ukey(au)
		if au == -1 {
			k = "x:badcreator"
		}
		add(k, hp, HomePartOut{Posts: byAuthor[au]})
	}
}

// describeOp renders a piece for failure reports.
func describeOp(op check.Op[any, any]) string {
	res := ""
	if !op.Unknown {
		switch o := op.Out.(type) {
		case WriteOut:
			res = fmt.Sprintf(" -> HTTP %d", o.Status)
		case ReadOut:
			res = fmt.Sprintf(" -> HTTP %d, %d posts", o.Status, len(o.Page))
		case HomePartOut:
			res = fmt.Sprintf(" -> %d posts by the author", len(o.Posts))
		}
	} else {
		res = " -> ? (outcome unknown)"
	}
	switch in := op.In.(type) {
	case Compose:
		return fmt.Sprintf("compose #%s by %d%s", in.Marker, in.Author, res)
	case Follow:
		verb := "unfollow"
		if in.On {
			verb = "follow"
		}
		return fmt.Sprintf("%s %d -> %d%s", verb, in.U, in.V, res)
	case Register:
		return fmt.Sprintf("register %d%s", in.ID, res)
	case Claim:
		return fmt.Sprintf("register of username %s%s", in.Name, res)
	case UserRead:
		return fmt.Sprintf("user timeline of %d [%d,%d)%s", in.User, in.Start, in.End, res)
	case HomeCheck:
		return fmt.Sprintf("home timeline of %d [%d,%d) (page)%s", in.Reader, in.Start, in.End, res)
	case HomePart:
		return fmt.Sprintf("home timeline of %d [%d,%d) (posts by %d)%s", in.Reader, in.Start, in.End, in.Author, res)
	case V2Compose:
		return fmt.Sprintf("compose #%s by %d%s", in.C.Marker, in.C.Author, res)
	case V2Follow:
		return fmt.Sprintf("follow change %d -> %d%s", in.F.U, in.F.V, res)
	case V2Read:
		return fmt.Sprintf("home timeline of %d [%d,%d)%s", in.Reader, in.Start, in.End, res)
	}
	return fmt.Sprintf("%v%s", op.In, res)
}
