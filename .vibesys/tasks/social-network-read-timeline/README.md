# Social Network read timeline

This task minimizes p50 scheduled-arrival latency for a read-heavy workload
against `socialNetwork/`: 50% user-timeline reads, 40% home-timeline reads, and
10% compose plus read-your-write sequences. The benchmark requires a 100%
success rate and coverage of every operation.

Run VibeSys from the repository root:

```bash
vibesys --task social-network-read-timeline
```

The candidate must be available at `http://localhost:8080` for evaluation. A
local setup is:

```bash
docker compose -f socialNetwork/docker-compose.yml up -d
python3 socialNetwork/scripts/init_social_graph.py \
  --graph=socfb-Reed98 --ip=localhost --port=8080
.vibesys/tasks/social-network-read-timeline/nginx_patches/apply_patches.sh
```

The nginx patch adds three check-only endpoints and request timing headers. It
must be reapplied whenever the nginx container is recreated. The task-specific
checker then verifies eleven API, ordering, visibility, mutation, pagination,
and read-your-write properties:

```bash
.vibesys/tasks/social-network-read-timeline/accuracy_checker/checker
```

The packaged `servicebench` entry point provides the benchmark. Its workload
is `.vibesys/tasks/social-network-read-timeline/benchmark/workload.toml`.
The checker builds into `/tmp/vibesys-social-network-read-timeline/`, outside
the read-only task tree.
