# Social Network benchmark adapter

Requests over the task's dataset (`internal/dataset`, seeded by `cmd/seed`)
and exact validation of every answer.

Operations (workload weights 500/400/100/5/5):

- `user_timeline_read`: the user timeline of a user drawn by Zipf (s = 1)
  over follower count (profiles of popular users are viewed most).
- `home_timeline_read`: the home timeline of a user drawn by Zipf (s = 0.75)
  over post count (active users read and post most).
- `compose_post`: an active user posts (a few words from a short list, a
  unique `#m...` marker, a mention of a followee 20% of the time, a URL at
  the end 15%, media 5%), then reads their own user timeline, which must
  hold the post (read your write).
- `follow` / `unfollow`: an active user follows a popular user, or drops a
  followee, on an edge with no change in flight.

Pages are 10 posts: the first 80%, the second 12%, the third 5%, pages 4-7
3%. Over 100k samples an unbounded cache keyed by the exact request would hit
66.7% of the timeline requests (user timelines 77%, home timelines 57%; see
`TestRequestDistribution`).

Validation (`Model`, `internal/timeline`) is exact under the contract in
`evaluator/INTERFACE.md`: seeded posts must be served with their stored
content; live posts are matched by marker on first sight, checked against
what was sent (creator, text with the URL shortened, mentions, media, a
timestamp within the compose's time) and must keep that identity. A page
must be `timeline[start:stop]` of some timeline that holds every post that
must be there, any of the posts that may be there, and nothing else:
acknowledged posts are required in the author's timeline immediately and in
followers' and mentioned users' home timelines for reads starting more than
1 s after the acknowledgement; requests in flight, failed writes and posts
composed around a follow change are allowed either way. Times are taken
conservatively (a read starts no earlier than its plan was built, a write
is acknowledged no later than its validation), so slack only ever favours
the application.

The model carries over between trials; `--state-out`/`--state-in` of
`cmd/loadgen` carry it between processes (the journal of writes, about
10 bytes per write event compressed).
