package hotelspec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"iter"
	"slices"
	"sort"

	"github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go/check"
)

const (
	msgRate     = "Rate updated."
	msgProfile  = "Profile updated."
	msgCapacity = "Capacity updated."
	msgLoginOK  = "Login successfully!"
)

type (
	piece      = check.Piece[In, Out]
	projection = check.Projection[In, Out]
)

func key(id string) []string { return []string{"h:" + id} }

// staticKey holds the pieces of operations that depend on no hotel's state (a lost login):
// they are unknown reads, which the check drops after counting them.
var staticKey = []string{"x:static"}

// project turns the history's calls into pieces: one per hotel an answer concerns. A response
// that is wrong without any state (a malformed body, a hotel that is not seeded, a status the
// operation never answers) is a violation.
func project(cat Catalog) func(iter.Seq[[]check.Call], *projection) error {
	return func(phases iter.Seq[[]check.Call], out *projection) error {
		d := &decoder{cat: cat, out: out}
		for calls := range phases {
			for i := range calls {
				if err := d.call(&calls[i]); err != nil {
					return fmt.Errorf("%s %d of phase %d: %w", calls[i].Op, calls[i].Index, calls[i].Phase, err)
				}
			}
		}
		return nil
	}
}

type decoder struct {
	cat Catalog
	out *projection
}

func (d *decoder) hotel(id string) (*Hotel, bool) { return d.cat.Hotel(id) }

// info decodes a call's Info strictly.
func info(c *check.Call, v any) error {
	dec := json.NewDecoder(bytes.NewReader(c.Info))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("info %s: %w", truncate(c.Info), err)
	}
	return nil
}

// outcome returns the call's single step and whether its outcome is unknown: no response or a
// 5xx, so a write may or may not have taken effect.
func outcome(c *check.Call) (step check.Step, unknown bool, err error) {
	if len(c.Steps) > 1 {
		return step, false, fmt.Errorf("%d steps, want 1", len(c.Steps))
	}
	if c.Lost() {
		return step, true, nil
	}
	step = c.Steps[0]
	return step, step.Status >= 500, nil
}

func truncate[T ~string | ~[]byte](b T) string {
	if len(b) > 200 {
		return string(b[:200]) + "..."
	}
	return string(b)
}

func object(raw any, want []string, what string) (map[string]any, error) {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s is not an object", what)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if !slices.Equal(keys, want) {
		return nil, fmt.Errorf("%s fields %q, want %q", what, keys, want)
	}
	return m, nil
}

func message(body []byte) (string, error) {
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		return "", fmt.Errorf("body is not JSON: %q", truncate(body))
	}
	m, err := object(v, []string{"message"}, "response")
	if err != nil {
		return "", err
	}
	s, ok := m["message"].(string)
	if !ok {
		return "", fmt.Errorf("message %v is not a string", m["message"])
	}
	return s, nil
}

type feature struct {
	id      string
	profile Profile
}

// features parses a strict GeoJSON FeatureCollection of seeded hotels.
func (d *decoder) features(body []byte) ([]feature, error) {
	var root any
	if err := json.Unmarshal(body, &root); err != nil {
		return nil, fmt.Errorf("body is not JSON: %q", truncate(body))
	}
	fields, err := object(root, []string{"features", "type"}, "GeoJSON root")
	if err != nil {
		return nil, err
	}
	list, ok := fields["features"].([]any)
	if fields["type"] != "FeatureCollection" || !ok {
		return nil, fmt.Errorf("not a FeatureCollection")
	}
	var out []feature
	seen := map[string]bool{}
	for n, raw := range list {
		f, err := object(raw, []string{"geometry", "id", "properties", "type"}, fmt.Sprintf("feature %d", n))
		if err != nil {
			return nil, err
		}
		id, _ := f["id"].(string)
		h, ok := d.hotel(id)
		if f["type"] != "Feature" || !ok {
			return nil, fmt.Errorf("feature %d: type %v id %v (unknown hotel)", n, f["type"], f["id"])
		}
		if seen[id] {
			return nil, fmt.Errorf("hotel %s twice", id)
		}
		seen[id] = true
		props, err := object(f["properties"], []string{"name", "phone_number"}, "properties")
		if err != nil {
			return nil, err
		}
		name, ok1 := props["name"].(string)
		phone, ok2 := props["phone_number"].(string)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("hotel %s: properties %v", id, props)
		}
		geo, err := object(f["geometry"], []string{"coordinates", "type"}, "geometry")
		if err != nil {
			return nil, err
		}
		coords, ok := geo["coordinates"].([]any)
		if geo["type"] != "Point" || !ok || len(coords) != 2 || coords[0] != any(h.Lon) || coords[1] != any(h.Lat) {
			return nil, fmt.Errorf("hotel %s: geometry %v, want [%v %v]", id, geo, h.Lon, h.Lat)
		}
		out = append(out, feature{id, Profile{Name: name, Phone: phone}})
	}
	return out, nil
}

// rates parses GET /rates strictly against the seeded plans and returns the prices by code.
func (d *decoder) rates(body []byte, id string) ([]PlanPrices, error) {
	h, ok := d.hotel(id)
	if !ok {
		return nil, fmt.Errorf("unknown hotel")
	}
	var root any
	if err := json.Unmarshal(body, &root); err != nil {
		return nil, fmt.Errorf("body is not JSON")
	}
	fields, err := object(root, []string{"hotelId", "ratePlans"}, "rates response")
	if err != nil {
		return nil, err
	}
	plans, ok := fields["ratePlans"].([]any)
	if fields["hotelId"] != id || !ok || len(plans) != len(h.Plans) {
		return nil, fmt.Errorf("hotelId %v and %d plans, want %s and %d", fields["hotelId"], len(plans), id, len(h.Plans))
	}
	byCode := map[string]*Plan{}
	for i := range h.Plans {
		byCode[h.Plans[i].Code] = &h.Plans[i]
	}
	var out []PlanPrices
	for n, raw := range plans {
		p, err := object(raw, []string{"code", "inDate", "outDate", "roomType"}, fmt.Sprintf("plan %d", n))
		if err != nil {
			return nil, err
		}
		code, _ := p["code"].(string)
		plan := byCode[code]
		if plan == nil {
			return nil, fmt.Errorf("plan %v unknown or twice", p["code"])
		}
		delete(byCode, code)
		if p["inDate"] != plan.InDate || p["outDate"] != plan.OutDate {
			return nil, fmt.Errorf("plan %s dates %v-%v", code, p["inDate"], p["outDate"])
		}
		room, err := object(p["roomType"], []string{"bookableRate", "code", "roomDescription", "totalRate", "totalRateInclusive"}, "roomType")
		if err != nil {
			return nil, err
		}
		if room["code"] != plan.RoomCode || room["roomDescription"] != plan.RoomDescription {
			return nil, fmt.Errorf("plan %s room %v", code, room["code"])
		}
		b, ok1 := room["bookableRate"].(float64)
		t, ok2 := room["totalRate"].(float64)
		i, ok3 := room["totalRateInclusive"].(float64)
		if !ok1 || !ok2 || !ok3 {
			return nil, fmt.Errorf("plan %s prices are not numbers", code)
		}
		out = append(out, PlanPrices{Code: code, Prices: Prices{Bookable: b, Total: t, Inclusive: i}})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Code < out[j].Code })
	return out, nil
}

// call projects one call. An error is a bug of the workload (an Info that does not decode, an
// operation this oracle does not know), not of the candidate.
func (d *decoder) call(c *check.Call) error {
	step, unknown, err := outcome(c)
	if err != nil {
		return err
	}
	bad := func(format string, args ...any) { d.out.Violation(c, format, args...) }
	add := func(p piece) {
		p.Origin = c
		p.Unknown = unknown
		d.out.Add(p)
	}
	switch c.Op {
	case "search":
		var in SearchInfo
		if err := info(c, &in); err != nil {
			return err
		}
		if unknown {
			for _, id := range in.Candidates {
				add(piece{Keys: key(id), In: In{Kind: Search, Hotel: id, Nights: in.Nights}})
			}
			return nil
		}
		if step.Status != 200 {
			bad("search answered HTTP %d: %q", step.Status, truncate(step.Body))
			return nil
		}
		fs, err := d.features(step.Body)
		if err != nil {
			bad("search: %v", err)
			return nil
		}
		shown := map[string]Profile{}
		for _, f := range fs {
			if !slices.Contains(in.Candidates, f.id) {
				bad("search answered hotel %s, not among geo's nearest %v", f.id, in.Candidates)
				return nil
			}
			shown[f.id] = f.profile
		}
		for _, id := range in.Candidates {
			p, present := shown[id]
			add(piece{Keys: key(id), In: In{Kind: Search, Hotel: id, Nights: in.Nights}, Out: Out{Present: present, Profile: p}})
		}
	case "reserve", "reserve_capacity_hotel":
		var in ReserveInfo
		if err := info(c, &in); err != nil {
			return err
		}
		p := piece{Keys: key(in.Hotel), Write: true, In: In{Kind: Reserve, Hotel: in.Hotel, Nights: in.Nights, Rooms: in.Rooms, ValidUser: in.Valid}}
		if !unknown {
			msg, err := message(step.Body)
			if step.Status != 200 || err != nil {
				bad("reservation answered HTTP %d %v", step.Status, err)
				return nil
			}
			if msg != MsgReserved && msg != MsgFull && msg != MsgLoginFail {
				bad("reservation answered %q", msg)
				return nil
			}
			p.Out.Msg = msg
		}
		add(p)
	case "set_capacity":
		var in CapacityInfo
		if err := info(c, &in); err != nil {
			return err
		}
		p := piece{Keys: key(in.Hotel), Write: true, In: In{Kind: SetCapacity, Hotel: in.Hotel, Rooms: in.Rooms}}
		switch {
		case unknown:
		case step.Status == 409:
			p.Out.Msg = MsgConflict
		case step.Status == 200:
			if msg, err := message(step.Body); err != nil || msg != msgCapacity {
				bad("capacity write answered %q %v", msg, err)
				return nil
			}
			p.Out.Msg = MsgOK
		default:
			bad("capacity write answered HTTP %d", step.Status)
			return nil
		}
		add(p)
	case "set_profile":
		var in ProfileInfo
		if err := info(c, &in); err != nil {
			return err
		}
		p := piece{Keys: key(in.Hotel), Write: true, In: In{Kind: SetProfile, Hotel: in.Hotel, Name: in.Name, Phone: in.Phone}}
		if !unknown {
			if msg, err := message(step.Body); step.Status != 200 || err != nil || msg != msgProfile {
				bad("profile write answered HTTP %d %q %v", step.Status, msg, err)
				return nil
			}
			p.Out.Msg = MsgOK
		}
		add(p)
	case "set_rate":
		var in RateInfo
		if err := info(c, &in); err != nil {
			return err
		}
		p := piece{Keys: key(in.Hotel), Write: true, In: In{Kind: SetRate, Hotel: in.Hotel, Code: in.Code, Prices: Prices{Bookable: in.Prices[0], Total: in.Prices[1], Inclusive: in.Prices[2]}}}
		if !unknown {
			if msg, err := message(step.Body); step.Status != 200 || err != nil || msg != msgRate {
				bad("rate write answered HTTP %d %q %v", step.Status, msg, err)
				return nil
			}
			p.Out.Msg = MsgOK
		}
		add(p)
	case "rates":
		var in HotelInfo
		if err := info(c, &in); err != nil {
			return err
		}
		p := piece{Keys: key(in.Hotel), In: In{Kind: ReadRates, Hotel: in.Hotel}}
		if !unknown {
			if step.Status != 200 {
				bad("rates of hotel %s answered HTTP %d", in.Hotel, step.Status)
				return nil
			}
			rates, err := d.rates(step.Body, in.Hotel)
			if err != nil {
				bad("rates of hotel %s: %v", in.Hotel, err)
				return nil
			}
			p.Out.Rates = rates
		}
		add(p)
	case "recommend_dis", "recommend_rate", "recommend_price":
		var in RecommendInfo
		if err := info(c, &in); err != nil {
			return err
		}
		if unknown {
			add(piece{Keys: staticKey, In: In{Kind: ReadProfile}})
			return nil
		}
		if step.Status != 200 {
			bad("recommendation answered HTTP %d", step.Status)
			return nil
		}
		fs, err := d.features(step.Body)
		if err != nil {
			bad("recommendation: %v", err)
			return nil
		}
		var ids []string
		var pieces []piece
		for _, f := range fs {
			ids = append(ids, f.id)
			pieces = append(pieces, piece{Keys: key(f.id), In: In{Kind: ReadProfile, Hotel: f.id}, Out: Out{Profile: f.profile}})
		}
		sort.Strings(ids)
		want := slices.Clone(in.Expected)
		sort.Strings(want)
		if !slices.Equal(ids, want) {
			bad("%s recommendation answered %v, want %v", in.Require, ids, want)
			return nil
		}
		for _, p := range pieces {
			add(p)
		}
	case "login":
		var in LoginInfo
		if err := info(c, &in); err != nil {
			return err
		}
		if unknown {
			add(piece{Keys: staticKey, In: In{Kind: ReadProfile}})
			return nil
		}
		want := MsgLoginFail
		if in.Valid {
			want = msgLoginOK
		}
		if msg, err := message(step.Body); step.Status != 200 || err != nil || msg != want {
			bad("login answered HTTP %d %q %v, want %q", step.Status, msg, err, want)
		}
	default:
		return fmt.Errorf("unknown operation %q", c.Op)
	}
	return nil
}
