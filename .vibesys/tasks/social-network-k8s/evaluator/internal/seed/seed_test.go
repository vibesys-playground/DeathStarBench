package seed

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
)

func smallDataset() *dataset.Dataset {
	return dataset.Generate(3, dataset.Config{Users: 300, FolloweeScale: 6, MaxFollowees: 100, Reciprocity: 0.2, PostScale: 3, MaxPosts: 50})
}

// The readers check BSON types (BSON_ITER_HOLDS_INT64, v_int64), so every
// integer must be int64 except post_type (int32).
func TestDocumentTypesMatchTheServices(t *testing.T) {
	data := smallDataset()
	want := map[string]bsontype.Type{
		"user_id": bson.TypeInt64, "post_id": bson.TypeInt64, "timestamp": bson.TypeInt64, "req_id": bson.TypeInt64,
		"media_id": bson.TypeInt64, "post_type": bson.TypeInt32,
	}
	var walk func(path string, raw bson.Raw)
	seen := map[string]bool{}
	walk = func(path string, raw bson.Raw) {
		elements, err := raw.Elements()
		if err != nil {
			t.Fatal(err)
		}
		for _, element := range elements {
			key, value := element.Key(), element.Value()
			if expected, ok := want[key]; ok {
				seen[key] = true
				if value.Type != expected {
					t.Fatalf("%s.%s has BSON type %v, want %v", path, key, value.Type, expected)
				}
			}
			switch value.Type {
			case bson.TypeEmbeddedDocument:
				walk(path+"."+key, value.Document())
			case bson.TypeArray:
				walk(path+"."+key, bson.Raw(value.Array()))
			}
		}
	}
	for _, collection := range MongoCollections(data) {
		for index := 0; index < collection.Count; index++ {
			raw, err := bson.Marshal(collection.Document(index))
			if err != nil {
				t.Fatal(err)
			}
			walk(collection.Database, raw)
		}
	}
	for key := range want {
		if !seen[key] {
			t.Errorf("no document has field %s", key)
		}
	}
}

func TestWriteSeedsEmptyDataTierOnce(t *testing.T) {
	data := smallDataset()
	mongos, redises, servers := fakeDataTier(t, data)
	results, skipped, err := Write(context.Background(), data, mongos, redises)
	if err != nil || skipped {
		t.Fatalf("Write = %v, skipped %v", err, skipped)
	}
	if len(results) != len(Targets(data)) {
		t.Fatalf("results %+v", results)
	}
	posts := mongos["post-storage-mongodb"].(*fakeMongo)
	if got := len(posts.collections["post.post"]); got != len(data.Posts) {
		t.Fatalf("posts written %d, want %d", got, len(data.Posts))
	}
	if !posts.indexes["post.post/post_id"] {
		t.Fatal("post_id index missing")
	}
	home := servers["home-timeline-redis"]
	for u := range data.Users {
		row := data.Home.Row(u)
		set := home.sets[strconv.FormatInt(data.Users[u].ID, 10)]
		if len(set) != len(row) {
			t.Fatalf("home timeline of user %d has %d entries, want %d", u, len(set), len(row))
		}
		for _, p := range row {
			if set[strconv.FormatInt(data.Posts[p].ID, 10)] != data.Posts[p].Timestamp {
				t.Fatalf("home timeline of user %d lacks post %d", u, p)
			}
		}
	}
	followers := servers["social-graph-redis"].sets[strconv.FormatInt(data.Users[data.PopularityRank[0]].ID, 10)+":followers"]
	if len(followers) != len(data.Followers.Row(int(data.PopularityRank[0]))) {
		t.Fatalf("followers set has %d members", len(followers))
	}

	if _, skipped, err := Write(context.Background(), data, mongos, redises); err != nil || !skipped {
		t.Fatalf("second Write = %v, skipped %v; want skipped", err, skipped)
	}
	if _, _, err := Write(context.Background(), smallDatasetOtherSeed(), mongos, redises); !errors.Is(err, ErrNotEmpty) {
		t.Fatalf("Write of another dataset = %v, want ErrNotEmpty", err)
	}
}

func smallDatasetOtherSeed() *dataset.Dataset {
	return dataset.Generate(4, smallDataset().Config)
}

func TestWriteRefusesNonEmptyDataTier(t *testing.T) {
	data := smallDataset()
	mongos, redises, servers := fakeDataTier(t, data)
	servers["home-timeline-redis"].sets["1"] = map[string]int64{"5": 1}
	if _, _, err := Write(context.Background(), data, mongos, redises); !errors.Is(err, ErrNotEmpty) {
		t.Fatalf("Write = %v, want ErrNotEmpty", err)
	}
	mongos, redises, _ = fakeDataTier(t, data)
	mongos["user-mongodb"].(*fakeMongo).collections["user.user"] = []any{bson.D{}}
	if _, _, err := Write(context.Background(), data, mongos, redises); !errors.Is(err, ErrNotEmpty) {
		t.Fatalf("Write = %v, want ErrNotEmpty", err)
	}
}

func fakeDataTier(t *testing.T, data *dataset.Dataset) (map[string]MongoServer, map[string]RedisServer, map[string]*fakeRedis) {
	mongos := map[string]MongoServer{}
	redises := map[string]RedisServer{}
	servers := map[string]*fakeRedis{}
	for _, collection := range MongoCollections(data) {
		mongos[collection.Target] = &fakeMongo{collections: map[string][]any{}, indexes: map[string]bool{}}
	}
	for _, set := range RedisSets(data) {
		server := startFakeRedis(t)
		client, err := DialRedis(context.Background(), server.address)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { client.Close() })
		redises[set.Target] = client
		servers[set.Target] = server
	}
	return mongos, redises, servers
}

type fakeMongo struct {
	mu          sync.Mutex
	collections map[string][]any
	indexes     map[string]bool
	marker      string
}

func (f *fakeMongo) Count(_ context.Context, database, collection string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return int64(len(f.collections[database+"."+collection])), nil
}

func (f *fakeMongo) Insert(_ context.Context, database, collection string, documents []any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.collections[database+"."+collection] = append(f.collections[database+"."+collection], documents...)
	return nil
}

func (f *fakeMongo) UniqueIndex(_ context.Context, database, collection, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.indexes[database+"."+collection+"/"+key] = true
	return nil
}

func (f *fakeMongo) Marker(context.Context) (string, error) { return f.marker, nil }

func (f *fakeMongo) SetMarker(_ context.Context, digest string) error {
	f.marker = digest
	return nil
}

// fakeRedis serves PING, DBSIZE and ZADD over RESP.
type fakeRedis struct {
	address string
	mu      sync.Mutex
	sets    map[string]map[string]int64
}

func startFakeRedis(t *testing.T) *fakeRedis {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	server := &fakeRedis{address: listener.Addr().String(), sets: map[string]map[string]int64{}}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go server.serve(conn)
		}
	}()
	return server
}

func (f *fakeRedis) serve(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	for {
		command, err := readCommand(reader)
		if err != nil {
			return
		}
		f.mu.Lock()
		switch strings.ToUpper(command[0]) {
		case "PING":
			writer.WriteString("+PONG\r\n")
		case "DBSIZE":
			fmt.Fprintf(writer, ":%d\r\n", len(f.sets))
		case "ZADD":
			set := f.sets[command[1]]
			if set == nil {
				set = map[string]int64{}
				f.sets[command[1]] = set
			}
			added := 0
			for i := 2; i+1 < len(command); i += 2 {
				score, _ := strconv.ParseInt(command[i], 10, 64)
				if _, ok := set[command[i+1]]; !ok {
					added++
				}
				set[command[i+1]] = score
			}
			fmt.Fprintf(writer, ":%d\r\n", added)
		default:
			writer.WriteString("-ERR unknown command\r\n")
		}
		f.mu.Unlock()
		if reader.Buffered() == 0 {
			writer.Flush()
		}
	}
}

func readCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, err
	}
	command := make([]string, count)
	for index := range command {
		header, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, _ := strconv.Atoi(strings.TrimSpace(header[1:]))
		buffer := make([]byte, size+2)
		if _, err := io.ReadFull(reader, buffer); err != nil {
			return nil, err
		}
		command[index] = string(buffer[:size])
	}
	return command, nil
}

func TestRedisClientDecodesReplies(t *testing.T) {
	server := startFakeRedis(t)
	client, err := DialRedis(context.Background(), server.address)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	replies, err := client.Pipeline([][]string{{"ZADD", "k", "1", "a", "2", "b"}, {"DBSIZE"}, {"NOPE"}})
	if err != nil {
		t.Fatal(err)
	}
	if replies[0] != int64(2) || replies[1] != int64(1) {
		t.Fatalf("replies %v", replies)
	}
	if _, ok := replies[2].(ErrorReply); !ok {
		t.Fatalf("error reply decoded as %T", replies[2])
	}
}
