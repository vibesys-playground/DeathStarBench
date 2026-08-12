# VibeSys tasks

This fork carries repository-native VibeSys tasks. Run VibeSys from the
repository root and select one of the directories under `tasks/`:

```bash
vibesys --task hotel-reservation
vibesys --task social-network-read-timeline
```

Task definitions and `evaluators.lock` are authored input. VibeSys owns
generated run data under `state/`; local-only data under `state/local/` is
ignored by Git.
