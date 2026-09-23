# ISUCON12 final setup status (2026-09-23)

`isucon-1` through `isucon-5` are five independent installations. Each host runs
`isuconquest.go`, nginx, and local MySQL. nginx proxies to `localhost:8080` and
the Go application uses local MySQL. The Taskfile roles cover all five hosts.
The current entry host for manual measurement is `isucon-1`.

## Official constraints

The [official manual](../../docs/official/manual.md) specifies portal initiated
benchmarks: `POST /initialize` has a 60 second limit, followed by consistency
checks and a 60 second load phase. A wrong response or a timeout loses 15
points; more than 50 such failures ends the run. `/admin` requests are checked
but earn no points. The final result uses the best score among the organizer's
repeated runs after a reboot. No benchmark was run during setup.

## Reset and deployment

- `task setup` acquired the Go implementation, nginx/MySQL/systemd/sysctl
  configuration, and official SQL assets from `isucon-1`. Checksums of the
  active Go source and key configurations matched on all five hosts.
- `task setup-preflight` and `task db-recreate-dry` passed. The reset graph
  stops all five Go services, uploads SQL to all five hosts, drops each local
  `isucon` database, applies `0_setup.sql`, `1_schema.sql`, and `2_init.sql`,
  starts the Go services, then invokes `POST /initialize` on every host.
  `init.sh` reads `3_schema_exclude_user_presents.sql`,
  `4_alldata_exclude_user_presents.sql`, and
  `5_user_presents_not_receive_data.tsv`. Each live database had 21 tables
  during the read only inspection. `task db-recreate` was **not** executed.
- `task setup-check`, `task deploy-all`, `task apply-roles`, `task check-roles`,
  and `task check-network` passed. Deployment restarted MySQL and Go and
  reloaded nginx on all five hosts.

## Measurement paths

The table below records the setup smoke checks. Smoke output is under
`.task/setup-smoke-20260923-231720/`; it is not a benchmark RUN. The later
benchmark result and measurement status are recorded in the baseline section.

| Measurement | Host, producer and enable condition | RUN output and reader | Smoke evidence |
| --- | --- | --- | --- |
| Score, pass/fail | `task bench` runs the benchmarker on `isucon-bench`; manual portal results can still be entered with `task bench-manual` | `bench.log`, `benchmark-result.txt` (manual only), `runs/scores.tsv`, `run.json`; DuckDB `runs` and `bench_summaries_raw`, dashboard score view | Manual recording path and parsers checked in code/tests; no actual result exists |
| Go CPU, heap, allocs, goroutine, fgprof | All app hosts; loopback `:6060`, `profiles` declarations enabled | Host named `.pprof` files; `go tool pprof`, DuckDB pprof tables, dashboard profile view | 25/25 files parsed; CPU and fgprof had nonzero samples on `isucon-1` |
| SQL pool | All app hosts; `GET /debug/sql-pools`, collector enabled | Host named `sql-pool-metrics.tsv`; DuckDB `metrics_sql_pool`, dashboard resources | 91 data rows per host during smoke |
| nginx access, alp, upstream | All nginx hosts; JSON access log and RUN rotation enabled | `raw/access-<host>.log.zst`, `alp.json`, `upstream-breakdown.tsv`, ingress outputs; DuckDB HTTP/endpoints/upstreams and dashboard | Three real requests on `isucon-1`, empty compressed logs on the other four hosts; alp and upstream had data |
| User transitions | All nginx hosts; authenticated Go middleware emits a shortened SHA-256 session pseudonym as a response header; digester enabled | `user-transitions.json`; DuckDB transition views and dashboard | Three classified APIs, two identified requests, one session and one transition; no raw session token saved |
| MySQL status, locks, slow queries, digest | All MySQL hosts; slow log at 100 ms and Performance Schema enabled | Host named status/lock/digest/slow query outputs; DuckDB MySQL views and dashboard | Seven status samples per host; digest rows on all five; low load slow log may be sparse |
| Host, service, disk | All five hosts; collectors enabled for `isuconquest.go`, nginx, MySQL | Host named proc/service/disk TSV files; DuckDB and dashboard resource views | Eight proc samples and 24 service rows per host |
| App/nginx/kernel/OOM faults | All relevant hosts; journal/error log digest in the marked load window | Host named journal/error/kernel/OOM logs; RUN artifacts and dashboard diagnostics | App journal contains the smoke requests; no nginx, kernel, or OOM event in the short window |

High frequency task state and nginx on CPU profiling are intentionally disabled:
the standard service and host metrics are active, and these additional sources
have no setup question requiring their cost. MySQL lock wait collection remains
enabled on all five database hosts.

## Baseline procedure

Run `task bench` to invoke the benchmarker on `isucon-bench` with
`--target-host=172.31.45.101 --stage=prod --request-timeout=10s`.
The command finalizes failed runs too and makes a local commit of the RUN and
`runs/scores.tsv`. Inspect the result with
`task artifacts-run RUN=runs/<RUN_ID>`, `task q-sync`, and the dashboard.
The first real RUN exposed the benchmark output format. Missing results remain
unknown rather than being recorded as zero.

The profile duration is 240 seconds with a snapshot at 75 seconds. Confirm
profile time coverage in the baseline RUN before treating the profiles as load
evidence.

## Baseline verification (2026-09-23)

- `20260923-233835` ended before load because SSH host key verification failed.
  It is finalized with `passed=false`. Five required pt-query-digest logs are
  missing; `task artifacts-run` correctly fails for this RUN.
- `20260923-234927` passed initialization and validation and completed load.
  The benchmark output reports `[PASSED]: true`, score 0, addition 260,
  deduction 300, and 20 completion-summary errors (16 timeouts and four HTTP
  500 responses). The original result is preserved in `bench.log`. The old
  output patterns left `run.json` and `scores.tsv` unknown; neither saved RUN
  was rewritten. The contest-specific patterns now capture future results,
  while DuckDB `bench_results`, `bench_score_routes`, and `bench_errors` expose
  this saved result. Dashboard scores, RUN list, and timeline were checked
  against the saved RUN through their HTTP APIs.
- `task artifacts-run RUN=runs/20260923-234927` passes. Its manifest declares
  108 artifacts: 104 `ok` and four optional empty pt-query-digest JSON files
  on `isucon-2` through `isucon-5`. The required 31 artifacts are present.
  Five hosts each have valid one-second proc, service, disk, MySQL status,
  and SQL pool series, about 126-127 samples per host. All 25 Go profiles are
  valid and imported, including a 240-second CPU and fgprof capture per host.
- nginx recorded 257 requests on entry host `isucon-1` (four HTTP 500s) and
  one request on `isucon-4`; the other three nginx logs are empty. Only
  `isucon-1` has slow-query rows. User transitions recorded 257 classified
  requests, nine identified sessions, and 69 transitions; 179 requests lacked
  a session identity, and one API request was unclassified.
- `analysis_windows` fell back to the full 126.771-second benchmark lifecycle:
  request rate did not reach its start threshold for two consecutive seconds.
  The actual load begins at 23:50:34 JST according to `bench.log`. Use that
  narrower interval when comparing load-only capacity or wait metrics. The
  existing `valid_runs` view still marks this archived RUN invalid because
  `run.json.passed` is unknown and four optional digest JSON files are empty.
  Read `bench_results` and artifact status separately for this RUN.
