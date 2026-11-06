# VibeSys tasks

This fork carries repository-native VibeSys tasks. Run VibeSys from the
repository root and select one of the directories under `tasks/`:

```bash
vibesys --task hotel-k8s
vibesys --task social-network-k8s
```

Both tasks use task schema 0.2 (`version = "0.2"`) without a `[candidate]` section: one task
image runs the check and the benchmark, and the task's own code deploys the application to
Kubernetes (this needs a vibesys CLI that accepts a 0.2 manifest without `[candidate]`). Each
task image installs vibesys-evaluator release 0.2.0rc1 (PyPI and the public uw-syfi/vibesys
module tags), pinned in its Dockerfile.

Task definitions are authored input. VibeSys owns
generated run data under `state/`; local-only data under `state/local/` is
ignored by Git.
