package snspec

// The reader's view, key "r:<reader>". It checks what the per-author projection cannot: the page
// ARITHMETIC of a home read (how many posts across all authors must or may precede the page,
// "page starts too new / too old", an empty page with posts that must be there). Its cost:
// every compose is replicated into the partition of every reader that has a home read and could
// receive it (follower, mentioned, or edge changed).

import (
	"fmt"
	"iter"
	"slices"
	"strconv"
	"time"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/check"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/oracle"
)

type V2Compose struct {
	C            Compose
	SeededFollow bool // dataset edge reader -> author
}
type V2Follow struct{ F Follow } // key: follower
type SeedCandA struct {
	SeedCand
	Author int64
}
type V2Read struct {
	Reader     int64
	Start, End int
	Holds      []string
	Row        []SeedCandA // seeded home row (prefix down to the page)
	Extra      []SeedCandA // seeded posts of authors whose edge changed and is not seeded
	Page       PageInfo
}

func rkey(u int64) string { return "r:" + strconv.FormatInt(u, 10) }

// ReaderSpec is the reader view over the seeded dataset d, with the bound and the clock slack of
// live post timestamps.
func ReaderSpec(d *dataset.Dataset, bound, slack time.Duration) *check.Spec[State, any, any] {
	m := NewModel(d, int64(bound), int64(slack))
	sp := AuthorSpec(d, bound, slack)
	sp.Name = "social-network/reader"
	sp.Project = m.projectReader
	sp.Step = func(s State, op check.Op[any, any]) ([]State, string) {
		out := op.Out
		if op.Unknown {
			out = unknownOut{}
		}
		n, why := m.stepReader(s, op.In, out, op.Call, op.Return)
		if why != "" {
			return nil, why
		}
		return []State{n}, ""
	}
	return sp
}

// DefaultReaderSpec uses the task's bound and slack.
func DefaultReaderSpec(d *dataset.Dataset) *check.Spec[State, any, any] {
	return ReaderSpec(d, Bound, oracle.DefaultSlack)
}

func (m *Model) projectReader(phases iter.Seq[[]check.Call], out *projection) error {
	h, err := collect(phases)
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
	// Readers with a checkable home read.
	type rd struct {
		ac *acall
		in V2Read
	}
	var reads []rd
	readers := map[int64]bool{}
	for i := range h.calls {
		ac := &h.calls[i]
		a := ac.a
		if a.Kind != oracle.KindRead || !a.Home || ac.unknown || ac.ro.Status != 200 || ac.ro.ParseErr != "" {
			continue
		}
		user, e1 := strconv.ParseInt(a.UserID, 10, 64)
		start, e2 := strconv.Atoi(a.Start)
		stop, e3 := strconv.Atoi(a.Stop)
		if e1 != nil || e2 != nil || e3 != nil || start < 0 || start >= stop || !(h.known[user] || m.seededUser(user) >= 0) {
			continue
		}
		info := pageInfo(ac.ro.Page)
		cutoff := int64(-1 << 62)
		if info.Len == stop-start {
			cutoff = info.Lo
		}
		in := V2Read{Reader: user, Start: start, End: stop, Holds: a.Holds, Page: info}
		if ui := m.seededUser(user); ui >= 0 {
			for _, p := range d.Home.Row(ui) {
				post := &d.Posts[p]
				if post.Timestamp < cutoff {
					break
				}
				sc := SeedCandA{SeedCand{Idx: p, TS: post.Timestamp, Mentioned: slices.Contains(post.Mentions, int32(ui))}, d.Users[post.Author].ID}
				in.Row = append(in.Row, sc)
			}
			for v := range h.changedBy[user] {
				if iv := m.seededUser(v); iv >= 0 && !d.Follows(ui, iv) {
					for _, p := range d.UserPosts.Row(iv) {
						post := &d.Posts[p]
						if post.Timestamp < cutoff {
							break
						}
						if !slices.Contains(post.Mentions, int32(ui)) {
							in.Extra = append(in.Extra, SeedCandA{SeedCand{Idx: p, TS: post.Timestamp}, v})
						}
					}
				}
			}
		}
		reads = append(reads, rd{ac, in})
		readers[user] = true
	}
	for _, r := range reads {
		out.Add(piece{Keys: []string{rkey(r.in.Reader)}, In: r.in, Out: r.ac.ro, Origin: r.ac.origin})
	}
	// Recipients of a compose by a: readers that follow (seeded), changed edge, or are mentioned.
	followersOf := map[int64][]int64{}
	for u := range readers {
		if ui := m.seededUser(u); ui >= 0 {
			for _, f := range d.Followees.Row(ui) {
				followersOf[d.Users[f].ID] = append(followersOf[d.Users[f].ID], u)
			}
		}
		for v := range h.changedBy[u] {
			followersOf[v] = append(followersOf[v], u)
		}
	}
	for i := range h.calls {
		ac := &h.calls[i]
		a := ac.a
		wo := WriteOut{Status: ac.status, Acked: ac.acked}
		switch a.Kind {
		case oracle.KindCompose:
			if a.Text == "" {
				continue
			}
			cp := Compose{Author: a.User, AuthorName: nameOf(a.User), Marker: a.Marker, Text: a.Text, URL: a.URL, Mentions: a.Mentions, Media: a.Media}
			for _, v := range a.Mentions {
				cp.MentionNames = append(cp.MentionNames, nameOf(v))
			}
			rec := map[int64]bool{}
			for _, u := range followersOf[a.User] {
				rec[u] = true
			}
			for _, v := range a.Mentions {
				if readers[v] {
					rec[v] = true
				}
			}
			for u := range rec {
				out.Add(piece{Keys: []string{rkey(u)}, In: V2Compose{cp, m.seededFollows(u, a.User)}, Out: wo, Write: true, Unknown: ac.unknown, Origin: ac.origin})
			}
		case oracle.KindFollow:
			if readers[a.User] {
				out.Add(piece{Keys: []string{rkey(a.User)}, In: V2Follow{Follow{U: a.User, V: a.Followee, On: a.On}}, Out: wo, Write: true, Unknown: ac.unknown, Origin: ac.origin})
			}
		}
	}
	return nil
}

func (m *Model) stepReader(s State, in, outAny any, call, ret int64) (State, string) {
	switch in := in.(type) {
	case V2Compose:
		// seeded-follow flag is kept next to the post in Edges' sibling map via Post.Mentions? keep simple:
		n, why := m.stepCompose(s, in.C, outAny, call, ret)
		if why == "" && in.SeededFollow {
			if n.Names == nil {
				n = n.cloneMaps()
			}
			n.Names[fmt.Sprint("sf:", in.C.Author)] = true // reuse Names as a set of seeded-followed authors
		}
		return n, why
	case V2Follow:
		n := s.cloneMaps()
		ok := !isUnknownOut(outAny)
		if ok && !outAny.(WriteOut).Acked {
			return s, fmt.Sprintf("follow %d -> %d: HTTP %d", in.F.U, in.F.V, outAny.(WriteOut).Status)
		}
		n.Edges[in.F.V] = append(slices.Clone(s.Edges[in.F.V]), Change{On: in.F.On, Sent: call, Acked: ret, OK: ok})
		return n, ""
	case V2Read:
		if isUnknownOut(outAny) {
			return s, ""
		}
		return m.stepV2Read(s, in, outAny.(ReadOut), call, ret)
	}
	panic(fmt.Sprintf("view2 piece %T", in))
}

func (m *Model) stepV2Read(s State, in V2Read, o ReadOut, call, ret int64) (State, string) {
	what := fmt.Sprintf("home timeline of %d [%d,%d) (reader view)", in.Reader, in.Start, in.End)
	r := window{call, ret}
	u := in.Reader
	changedBefore := map[int64]bool{}
	for a, chs := range s.Edges {
		for _, c := range chs {
			if c.Sent < r.end {
				changedBefore[a] = true
			}
		}
	}
	var cands []cand
	for _, sc := range in.Row {
		cands = append(cands, cand{seeded: sc.Idx, must: !changedBefore[sc.Author] || sc.Mentioned, ts: sc.TS})
	}
	for _, sc := range in.Extra {
		if changedBefore[sc.Author] {
			cands = append(cands, cand{seeded: sc.Idx, must: false, ts: sc.TS})
		}
	}
	for _, p := range s.Posts {
		var v vis
		if slices.Contains(p.Mentions, u) {
			v = must
		} else {
			v = m.deliveryC(s.Edges[p.Author], s.Names[fmt.Sprint("sf:", p.Author)], p.Sent, r)
		}
		if v == excluded {
			continue
		}
		if lv := liveVis(p, r, m.Bound, in.Holds); lv != excluded {
			cands = append(cands, cand{seeded: -1, live: p, must: v == must && lv == must, ts: learnedTS(p)})
		}
	}
	next, why := m.matchPage(s, what, cands, o.Page, r, in.Start, in.End, nil)
	if why != "" {
		return s, why
	}
	return next, m.windowFull(what, cands, o.Page, in.Start, in.End, r, next)
}
