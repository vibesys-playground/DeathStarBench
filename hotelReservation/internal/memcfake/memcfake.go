// Package memcfake is an in-process memcached for tests: the subset of the
// text protocol that gomemcache speaks (gets, set, add, replace, cas, delete,
// flush_all, version), with CAS identifiers. Expiration times are ignored.
package memcfake

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
)

type entry struct {
	flags uint32
	value []byte
	cas   uint64
}

// Server is a memcached listening on a local port.
type Server struct {
	listener net.Listener

	mu      sync.Mutex
	items   map[string]entry
	nextCAS uint64
	// BeforeStore, when set, runs before every storage command executes
	// (with the verb and key), outside the lock, so a test can interleave
	// other clients' commands at that point.
	BeforeStore func(verb, key string)
}

// Start listens on 127.0.0.1 and serves until Close.
func Start() (*Server, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	server := &Server{listener: listener, items: make(map[string]entry)}
	go server.accept()
	return server, nil
}

// Addr is the host:port to give the client.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// Close stops listening.
func (s *Server) Close() error { return s.listener.Close() }

// Value returns the stored value of key.
func (s *Server) Value(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.items[key]
	return item.value, ok
}

// Put stores a value directly.
func (s *Server) Put(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextCAS++
	s.items[key] = entry{value: append([]byte(nil), value...), cas: s.nextCAS}
}

// Flush empties the cache, as a restart of memcached does.
func (s *Server) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = make(map[string]entry)
}

func (s *Server) accept() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.serve(conn)
	}
}

func (s *Server) serve(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		fields := strings.Fields(strings.TrimRight(line, "\r\n"))
		if len(fields) == 0 {
			continue
		}
		switch verb := fields[0]; verb {
		case "get", "gets":
			s.mu.Lock()
			for _, key := range fields[1:] {
				if item, ok := s.items[key]; ok {
					fmt.Fprintf(writer, "VALUE %s %d %d %d\r\n", key, item.flags, len(item.value), item.cas)
					writer.Write(item.value)
					writer.WriteString("\r\n")
				}
			}
			s.mu.Unlock()
			writer.WriteString("END\r\n")
		case "set", "add", "replace", "cas":
			if len(fields) < 5 {
				writer.WriteString("ERROR\r\n")
				break
			}
			flags, _ := strconv.ParseUint(fields[2], 10, 32)
			size, err := strconv.Atoi(fields[4])
			if err != nil {
				writer.WriteString("ERROR\r\n")
				break
			}
			value := make([]byte, size+2)
			if _, err := io.ReadFull(reader, value); err != nil {
				return
			}
			var casID uint64
			if verb == "cas" && len(fields) > 5 {
				casID, _ = strconv.ParseUint(fields[5], 10, 64)
			}
			if s.BeforeStore != nil {
				s.BeforeStore(verb, fields[1])
			}
			writer.WriteString(s.store(verb, fields[1], uint32(flags), value[:size], casID) + "\r\n")
		case "delete":
			s.mu.Lock()
			_, ok := s.items[fields[1]]
			delete(s.items, fields[1])
			s.mu.Unlock()
			if ok {
				writer.WriteString("DELETED\r\n")
			} else {
				writer.WriteString("NOT_FOUND\r\n")
			}
		case "flush_all":
			s.Flush()
			writer.WriteString("OK\r\n")
		case "version":
			writer.WriteString("VERSION memcfake\r\n")
		default:
			writer.WriteString("ERROR\r\n")
		}
		if err := writer.Flush(); err != nil {
			return
		}
	}
}

func (s *Server) store(verb, key string, flags uint32, value []byte, casID uint64) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.items[key]
	switch verb {
	case "add":
		if exists {
			return "NOT_STORED"
		}
	case "replace":
		if !exists {
			return "NOT_STORED"
		}
	case "cas":
		if !exists {
			return "NOT_FOUND"
		}
		if current.cas != casID {
			return "EXISTS"
		}
	}
	s.nextCAS++
	s.items[key] = entry{flags: flags, value: append([]byte(nil), value...), cas: s.nextCAS}
	return "STORED"
}
