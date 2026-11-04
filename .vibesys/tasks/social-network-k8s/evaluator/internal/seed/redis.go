package seed

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

// Redis is a minimal RESP2 client: pipelined commands, replies decoded to
// int64, string, []any, nil or an error reply.
type Redis struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

// ErrorReply is a Redis error reply.
type ErrorReply string

func (e ErrorReply) Error() string { return "redis: " + string(e) }

// DialRedis connects to address, retrying until ctx ends.
func DialRedis(ctx context.Context, address string) (*Redis, error) {
	var dialer net.Dialer
	for {
		conn, err := dialer.DialContext(ctx, "tcp", address)
		if err == nil {
			client := &Redis{conn: conn, reader: bufio.NewReaderSize(conn, 1<<16), writer: bufio.NewWriterSize(conn, 1<<16)}
			if _, err = client.Do("PING"); err == nil {
				return client, nil
			}
			conn.Close()
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("connect redis %s: %w (last error: %v)", address, ctx.Err(), err)
		case <-time.After(time.Second):
		}
	}
}

// Close closes the connection.
func (r *Redis) Close() error { return r.conn.Close() }

// Do runs one command.
func (r *Redis) Do(args ...string) (any, error) {
	replies, err := r.Pipeline([][]string{args})
	if err != nil {
		return nil, err
	}
	if reply, ok := replies[0].(error); ok {
		return nil, reply
	}
	return replies[0], nil
}

// Pipeline sends the commands and reads one reply per command. Error replies
// are returned in place; err is a transport or protocol error.
func (r *Redis) Pipeline(commands [][]string) ([]any, error) {
	_ = r.conn.SetDeadline(time.Now().Add(60 * time.Second))
	for _, command := range commands {
		fmt.Fprintf(r.writer, "*%d\r\n", len(command))
		for _, arg := range command {
			fmt.Fprintf(r.writer, "$%d\r\n%s\r\n", len(arg), arg)
		}
	}
	if err := r.writer.Flush(); err != nil {
		return nil, err
	}
	replies := make([]any, len(commands))
	for index := range replies {
		reply, err := r.read()
		if err != nil {
			return nil, err
		}
		replies[index] = reply
	}
	return replies, nil
}

func (r *Redis) read() (any, error) {
	line, err := r.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 || line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("redis: malformed reply %q", line)
	}
	kind, body := line[0], line[1:len(line)-2]
	switch kind {
	case '+':
		return body, nil
	case '-':
		return ErrorReply(body), nil
	case ':':
		return strconv.ParseInt(body, 10, 64)
	case '$':
		size, err := strconv.Atoi(body)
		if err != nil {
			return nil, err
		}
		if size < 0 {
			return nil, nil
		}
		buffer := make([]byte, size+2)
		if _, err := io.ReadFull(r.reader, buffer); err != nil {
			return nil, err
		}
		return string(buffer[:size]), nil
	case '*':
		count, err := strconv.Atoi(body)
		if err != nil {
			return nil, err
		}
		if count < 0 {
			return nil, nil
		}
		items := make([]any, count)
		for index := range items {
			if items[index], err = r.read(); err != nil {
				return nil, err
			}
		}
		return items, nil
	}
	return nil, errors.New("redis: unknown reply type " + strconv.Quote(string(kind)))
}
