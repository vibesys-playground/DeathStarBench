package socialnetwork

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"
)

// The journal is the model's write history (composes and follow changes,
// with their times and outcomes) in the order the model applied it. A later
// phase replays it so it validates against the same state. Learned post
// identities are not kept: the next phase learns them again.

type eventKind byte

const (
	eventPostBegin eventKind = iota + 1
	eventPostEnd
	eventEdgeBegin
	eventEdgeEnd
)

type event struct {
	Kind   eventKind
	Time   int64 // unix ns
	U, V   int32
	OK     bool // outcome; for eventEdgeBegin the new state (follow)
	Seq    int64
	Random uint64
	Marker string
}

// MaxJournalBytes bounds the compressed journal (it travels in a ConfigMap).
const MaxJournalBytes = 900 << 10

const journalMagic = "snj1"

// WriteJournal returns the compressed journal of everything the model has
// applied, including any replayed journal.
func (a *Application) WriteJournal() ([]byte, error) {
	a.model.mu.Lock()
	events := append([]event(nil), a.model.journal...)
	a.model.mu.Unlock()
	var out bytes.Buffer
	zipped := gzip.NewWriter(&out)
	writer := bufio.NewWriter(zipped)
	writer.WriteString(journalMagic)
	namespaces := map[string]uint64{}
	var buffer [binary.MaxVarintLen64]byte
	uvarint := func(value uint64) { writer.Write(buffer[:binary.PutUvarint(buffer[:], value)]) }
	varint := func(value int64) { writer.Write(buffer[:binary.PutVarint(buffer[:], value)]) }
	previous := int64(0)
	for _, e := range events {
		writer.WriteByte(byte(e.Kind))
		varint((e.Time - previous) / 1000) // microseconds
		previous += (e.Time - previous) / 1000 * 1000
		flag := uint64(0)
		if e.OK {
			flag = 1
		}
		switch e.Kind {
		case eventPostBegin, eventPostEnd:
			suffix := strconv.FormatInt(e.Seq, 36)
			namespace := e.Marker[:len(e.Marker)-len(suffix)]
			index, ok := namespaces[namespace]
			if !ok {
				index = uint64(len(namespaces))
				namespaces[namespace] = index
				uvarint(index)
				uvarint(uint64(len(namespace)))
				writer.WriteString(namespace)
			} else {
				uvarint(index)
			}
			uvarint(uint64(e.Seq))
			if e.Kind == eventPostBegin {
				uvarint(uint64(e.U))
				binary.BigEndian.PutUint64(buffer[:8], e.Random)
				writer.Write(buffer[:8])
			} else {
				uvarint(flag)
			}
		case eventEdgeBegin, eventEdgeEnd:
			uvarint(uint64(e.U))
			uvarint(uint64(e.V))
			uvarint(flag)
		}
	}
	if err := errors.Join(writer.Flush(), zipped.Close()); err != nil {
		return nil, err
	}
	if out.Len() > MaxJournalBytes {
		return nil, fmt.Errorf("journal of %d events is %d bytes compressed, more than %d", len(events), out.Len(), MaxJournalBytes)
	}
	return out.Bytes(), nil
}

func readJournal(compressed []byte) ([]event, error) {
	zipped, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReader(zipped)
	magic := make([]byte, len(journalMagic))
	if _, err := io.ReadFull(reader, magic); err != nil || string(magic) != journalMagic {
		return nil, fmt.Errorf("not a journal")
	}
	var namespaces []string
	var events []event
	previous := int64(0)
	for {
		kind, err := reader.ReadByte()
		if errors.Is(err, io.EOF) {
			return events, nil
		}
		if err != nil {
			return nil, err
		}
		var fail error
		uvarint := func() uint64 {
			value, err := binary.ReadUvarint(reader)
			if err != nil && fail == nil {
				fail = err
			}
			return value
		}
		delta, err := binary.ReadVarint(reader)
		if err != nil {
			return nil, err
		}
		previous += delta * 1000
		e := event{Kind: eventKind(kind), Time: previous}
		switch e.Kind {
		case eventPostBegin, eventPostEnd:
			index := uvarint()
			if index == uint64(len(namespaces)) {
				name := make([]byte, uvarint())
				if _, err := io.ReadFull(reader, name); err != nil {
					return nil, err
				}
				namespaces = append(namespaces, string(name))
			} else if index > uint64(len(namespaces)) {
				return nil, fmt.Errorf("bad namespace index %d", index)
			}
			e.Seq = int64(uvarint())
			e.Marker = namespaces[index] + strconv.FormatInt(e.Seq, 36)
			if e.Kind == eventPostBegin {
				e.U = int32(uvarint())
				var random [8]byte
				if _, err := io.ReadFull(reader, random[:]); err != nil {
					return nil, err
				}
				e.Random = binary.BigEndian.Uint64(random[:])
			} else {
				e.OK = uvarint() == 1
			}
		case eventEdgeBegin, eventEdgeEnd:
			e.U, e.V, e.OK = int32(uvarint()), int32(uvarint()), uvarint() == 1
		default:
			return nil, fmt.Errorf("bad event kind %d", kind)
		}
		if fail != nil {
			return nil, fail
		}
		events = append(events, e)
	}
}

// replay applies a journal to the model, as if its writes had happened in
// this process.
func (a *Application) replay(compressed []byte) error {
	events, err := readJournal(compressed)
	if err != nil {
		return err
	}
	m := a.model
	m.mu.Lock()
	defer m.mu.Unlock()
	n := int32(len(a.data.Users))
	open := map[uint64]*change{}
	for index, e := range events {
		at := time.Unix(0, e.Time)
		if e.U < 0 || e.U >= n || e.V < 0 || e.V >= n {
			return fmt.Errorf("event %d: user out of range", index)
		}
		switch e.Kind {
		case eventPostBegin:
			p := newLivePost(a.data, e.U, e.Random, e.Seq, e.Marker)
			p.sentAt = at
			m.beginPost(p)
		case eventPostEnd:
			p := m.byMarker[e.Marker]
			if p == nil {
				return fmt.Errorf("event %d: end of unknown post %s", index, e.Marker)
			}
			m.endPost(p, e.OK, at)
		case eventEdgeBegin:
			open[edgeKey(e.U, e.V)] = m.beginChange(e.U, e.V, e.OK, at)
		case eventEdgeEnd:
			c := open[edgeKey(e.U, e.V)]
			if c == nil {
				return fmt.Errorf("event %d: end of unknown change", index)
			}
			m.endChange(c, e.OK, at)
		}
	}
	return nil
}
