package socialnetwork

import (
	"fmt"
	"strconv"
	"strings"

	"vibesys/social-network-k8s-evaluator/internal/dataset"
	"vibesys/social-network-k8s-evaluator/internal/timeline"
)

// Draw streams of one compose sample (dataset.Mix(random, stream)).
const (
	streamWords = 100 + iota
	streamMention
	streamURL
	streamMedia
)

var composeWords = strings.Fields(`time day way year week work home city road park river tree garden
	table chair window door book page music song game team ball field water coffee tea bread apple
	lunch market train bus bike plan idea photo color light sound paper box walk run read write cook
	paint build open start finish visit share watch listen learn help find good new big small long
	early late quiet happy busy warm cold green blue bright fresh simple today tomorrow again soon`)

// newLivePost draws a compose of author from the sample's random word: a few
// words, a unique marker token, mentions of followees, an URL at the end
// (the pinned text service drops text after the last URL), media.
func newLivePost(data *dataset.Dataset, author int32, random uint64, seq int64, marker string) *livePost {
	rng := dataset.NewRNG(random, "compose", 0)
	count := 2 + rng.Pareto2(5)
	if count > 25 {
		count = 25
	}
	parts := make([]string, 0, count+4)
	for i := 0; i < count; i++ {
		parts = append(parts, composeWords[rng.Intn(len(composeWords))])
	}
	p := &livePost{seq: seq, marker: marker, author: author, random: random}
	followees := data.Followees.Row(int(author))
	if len(followees) > 0 && dataset.Unit(random, streamMention) < 0.2 {
		v := followees[dataset.Mix(random, streamMention+50)%uint64(len(followees))]
		p.mentions = []int32{v}
		at := int(dataset.Mix(random, streamMention+51) % uint64(len(parts)+1))
		parts = append(parts[:at], append([]string{"@" + data.Users[v].Username}, parts[at:]...)...)
	}
	parts = append(parts, "#"+marker)
	if dataset.Unit(random, streamURL) < 0.15 {
		p.url = fmt.Sprintf("http://example%d.com", dataset.Mix(random, streamURL+50)%1000000)
		parts = append(parts, p.url)
	}
	p.text = strings.Join(parts, " ")
	if dataset.Unit(random, streamMedia) < 0.05 {
		items := 1 + int(dataset.Mix(random, streamMedia+50)%2)
		for i := 0; i < items; i++ {
			id := 1e16 + dataset.Mix(random, streamMedia+51+uint64(i))%9e16
			p.media = append(p.media, timeline.Media{ID: strconv.FormatUint(id, 10), Type: "png"})
		}
	}
	return p
}

// composeForm is the request body of a live post.
func composeForm(data *dataset.Dataset, p *livePost) map[string]string {
	ids := make([]string, len(p.media))
	types := make([]string, len(p.media))
	for i, item := range p.media {
		ids[i] = strconv.Quote(item.ID)
		types[i] = strconv.Quote(item.Type)
	}
	user := &data.Users[p.author]
	return map[string]string{
		"username": user.Username, "user_id": strconv.FormatInt(user.ID, 10), "text": p.text,
		"media_ids": "[" + strings.Join(ids, ",") + "]", "media_types": "[" + strings.Join(types, ",") + "]",
		"post_type": "0",
	}
}

// markerOf returns the marker token ("m...") of a live post's text, or "".
func markerOf(text string) string {
	at := strings.Index(text, "#m")
	if at < 0 {
		return ""
	}
	end := at + 1
	for end < len(text) && text[end] != ' ' {
		end++
	}
	return text[at+1 : end]
}
