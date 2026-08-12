# Hotel Reservation

This task optimizes this fork's `hotelReservation/` application for maximum
closed-loop logical operations per second. The workload preserves the canonical
search-heavy request mix and requires perfect semantic correctness.

Run VibeSys from the repository root:

```bash
vibesys --task hotel-reservation
```

The packaged evaluator owns candidate startup, restart, cleanup, correctness,
benchmarking, and telemetry capture. It starts the candidate with:

```bash
docker compose -f hotelReservation/docker-compose.yml up -d --build
```

Reservations persist in Compose volumes and have no deletion API. The evaluator
uses randomized future dates and runs `docker compose down -v` after each
managed evaluation. Generated telemetry is local run state under
`.vibesys/state/local/hotel-reservation/`.

The authored contract, workload, telemetry description, and reference metadata
are immutable task input. The optimization candidate is the repository outside
`.vibesys/`.
