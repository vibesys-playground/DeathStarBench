// Package seed turns the task's dataset into the documents and Redis sorted
// sets that the pinned Social Network services themselves write, and writes
// them into the data tier.
//
// Schemas (from the services' C++ sources; every integer has the type the
// readers check for, which is int64 unless noted):
//
//	user-mongodb          user.user                  {user_id, first_name, last_name, username, salt, password = sha256hex(password + salt)}
//	social-graph-mongodb  social-graph.social-graph  {user_id, followers: [{user_id, timestamp}], followees: [{user_id, timestamp}]}
//	post-storage-mongodb  post.post                  {post_id, timestamp, text, req_id, post_type (int32), creator: {user_id, username},
//	                                                  urls: [{shortened_url, expanded_url}], user_mentions: [{user_id, username}], media: [{media_id, media_type}]}
//	user-timeline-mongodb user-timeline.user-timeline {user_id, posts: [{post_id, timestamp}] newest first}   (users with posts only)
//	url-shorten-mongodb   url-shorten.url-shorten    {shortened_url, expanded_url}
//	home-timeline-redis   ZSET "<user_id>"            member "<post_id>", score timestamp
//	social-graph-redis    ZSET "<user_id>:followers"  member "<follower_id>", score follow timestamp
//	                      ZSET "<user_id>:followees"  member "<followee_id>", score follow timestamp
//
// The caches (memcached, user-timeline-redis) are left empty. The indexes
// are the ones the services create at startup plus those on the other keys the
// services look documents up by (user.username, user.user_id,
// user-timeline.user_id), as a production database would have them.
package seed

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"

	"go.mongodb.org/mongo-driver/bson"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
)

// Collection is one MongoDB collection's documents, built on demand.
type Collection struct {
	Target     string // data-tier Service name
	Database   string
	Name       string
	Count      int
	Document   func(index int) bson.D
	UniqueKeys []string // single-field unique indexes
}

// Member is one sorted-set entry.
type Member struct {
	Score  int64
	Member string
}

// SortedSets is one Redis server's sorted sets, built on demand.
type SortedSets struct {
	Target string
	Count  int
	Set    func(index int) (key string, members []Member)
}

// MongoCollections lists the dataset's MongoDB collections.
func MongoCollections(data *dataset.Dataset) []Collection {
	var withPosts []int
	var urls []dataset.URL
	for u := range data.Users {
		if len(data.UserPosts.Row(u)) > 0 {
			withPosts = append(withPosts, u)
		}
	}
	for _, post := range data.Posts {
		urls = append(urls, post.URLs...)
	}
	return []Collection{
		{Target: "user-mongodb", Database: "user", Name: "user", Count: len(data.Users),
			Document: func(index int) bson.D { return UserDocument(&data.Users[index]) }, UniqueKeys: []string{"username", "user_id"}},
		{Target: "social-graph-mongodb", Database: "social-graph", Name: "social-graph", Count: len(data.Users),
			Document: func(index int) bson.D { return SocialGraphDocument(data, index) }, UniqueKeys: []string{"user_id"}},
		{Target: "post-storage-mongodb", Database: "post", Name: "post", Count: len(data.Posts),
			Document: func(index int) bson.D { return PostDocument(data, &data.Posts[index]) }, UniqueKeys: []string{"post_id"}},
		{Target: "user-timeline-mongodb", Database: "user-timeline", Name: "user-timeline", Count: len(withPosts),
			Document: func(index int) bson.D { return UserTimelineDocument(data, withPosts[index]) }, UniqueKeys: []string{"user_id"}},
		{Target: "url-shorten-mongodb", Database: "url-shorten", Name: "url-shorten", Count: len(urls),
			Document: func(index int) bson.D {
				return bson.D{{Key: "shortened_url", Value: urls[index].Shortened}, {Key: "expanded_url", Value: urls[index].Expanded}}
			}, UniqueKeys: []string{"shortened_url"}},
	}
}

// RedisSets lists the dataset's Redis sorted sets.
func RedisSets(data *dataset.Dataset) []SortedSets {
	n := len(data.Users)
	return []SortedSets{
		{Target: "home-timeline-redis", Count: n, Set: func(u int) (string, []Member) {
			row := data.Home.Row(u)
			members := make([]Member, len(row))
			for i, p := range row {
				post := &data.Posts[p]
				members[i] = Member{Score: post.Timestamp, Member: strconv.FormatInt(post.ID, 10)}
			}
			return strconv.FormatInt(data.Users[u].ID, 10), members
		}},
		{Target: "social-graph-redis", Count: 2 * n, Set: func(index int) (string, []Member) {
			u := index / 2
			key := strconv.FormatInt(data.Users[u].ID, 10)
			var members []Member
			if index%2 == 0 {
				for _, v := range data.Followers.Row(u) {
					members = append(members, Member{Score: data.FollowTime(int(v), u), Member: strconv.FormatInt(data.Users[v].ID, 10)})
				}
				return key + ":followers", members
			}
			for _, v := range data.Followees.Row(u) {
				members = append(members, Member{Score: data.FollowTime(u, int(v)), Member: strconv.FormatInt(data.Users[v].ID, 10)})
			}
			return key + ":followees", members
		}},
	}
}

// UserDocument is a user as UserService::RegisterUserWithId stores it.
func UserDocument(user *dataset.User) bson.D {
	hash := sha256.Sum256([]byte(user.Password + user.Salt))
	return bson.D{
		{Key: "user_id", Value: user.ID},
		{Key: "first_name", Value: user.FirstName},
		{Key: "last_name", Value: user.LastName},
		{Key: "username", Value: user.Username},
		{Key: "salt", Value: user.Salt},
		{Key: "password", Value: hex.EncodeToString(hash[:])},
	}
}

// SocialGraphDocument is a user's edges as SocialGraphService stores them.
func SocialGraphDocument(data *dataset.Dataset, u int) bson.D {
	followers := bson.A{}
	for _, v := range data.Followers.Row(u) {
		followers = append(followers, bson.D{{Key: "user_id", Value: data.Users[v].ID}, {Key: "timestamp", Value: data.FollowTime(int(v), u)}})
	}
	followees := bson.A{}
	for _, v := range data.Followees.Row(u) {
		followees = append(followees, bson.D{{Key: "user_id", Value: data.Users[v].ID}, {Key: "timestamp", Value: data.FollowTime(u, int(v))}})
	}
	return bson.D{{Key: "user_id", Value: data.Users[u].ID}, {Key: "followers", Value: followers}, {Key: "followees", Value: followees}}
}

// PostDocument is a post as PostStorageService::StorePost stores it.
func PostDocument(data *dataset.Dataset, post *dataset.Post) bson.D {
	author := &data.Users[post.Author]
	urls := bson.A{}
	for _, url := range post.URLs {
		urls = append(urls, bson.D{{Key: "shortened_url", Value: url.Shortened}, {Key: "expanded_url", Value: url.Expanded}})
	}
	mentions := bson.A{}
	for _, v := range post.Mentions {
		mentions = append(mentions, bson.D{{Key: "user_id", Value: data.Users[v].ID}, {Key: "username", Value: data.Users[v].Username}})
	}
	media := bson.A{}
	for _, item := range post.Media {
		media = append(media, bson.D{{Key: "media_id", Value: item.ID}, {Key: "media_type", Value: item.Type}})
	}
	return bson.D{
		{Key: "post_id", Value: post.ID},
		{Key: "timestamp", Value: post.Timestamp},
		{Key: "text", Value: post.Text},
		{Key: "req_id", Value: post.ReqID},
		{Key: "post_type", Value: int32(0)},
		{Key: "creator", Value: bson.D{{Key: "user_id", Value: author.ID}, {Key: "username", Value: author.Username}}},
		{Key: "urls", Value: urls},
		{Key: "user_mentions", Value: mentions},
		{Key: "media", Value: media},
	}
}

// UserTimelineDocument is a user's post list as UserTimelineService stores
// it: newest first.
func UserTimelineDocument(data *dataset.Dataset, u int) bson.D {
	posts := bson.A{}
	for _, p := range data.UserPosts.Row(u) {
		posts = append(posts, bson.D{{Key: "post_id", Value: data.Posts[p].ID}, {Key: "timestamp", Value: data.Posts[p].Timestamp}})
	}
	return bson.D{{Key: "user_id", Value: data.Users[u].ID}, {Key: "posts", Value: posts}}
}
