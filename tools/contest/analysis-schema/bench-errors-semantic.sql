-- ISUCON12 final benchmark output, interpreted without changing saved RUNs.
create table if not exists bench_error_logs (
    run_id varchar, content varchar, source_artifact varchar
);

create or replace view bench_results as
select
    run_id,
    try_cast(regexp_extract(content, '\[SCORE\] ([0-9]+) \(addition:', 1) as bigint) as score,
    try_cast(regexp_extract(content, '\[PASSED\]: (true|false)', 1) as boolean) as passed,
    try_cast(regexp_extract(content, '\[SCORE\] [0-9]+ \(addition: ([0-9]+)', 1) as bigint) as addition,
    try_cast(regexp_extract(content, '\[SCORE\] [0-9]+ \(addition: [0-9]+, deduction: ([0-9]+)', 1) as bigint) as deduction,
    source_artifact
from bench_error_logs;

create or replace view bench_score_routes as
with route_maps as (
    select run_id, source_artifact,
        regexp_extract(content, '\[SCORE\] map\[([^\]]+)\]', 1) as route_map
    from bench_error_logs
), route_matches as (
    select run_id, source_artifact,
        unnest(regexp_extract_all(route_map,
            '(GET|POST|PUT|PATCH|DELETE) ([^ ]+):([0-9]+)',
            ['method', 'route', 'points'])) as matched
    from route_maps
    where route_map <> ''
)
select run_id, matched.method as method, matched.route as route,
    try_cast(matched.points as bigint) as points, source_artifact
from route_matches;

-- ERROR lines are emitted together at completion. Their timestamp is the
-- reporting time, not the time at which each request failed.
create or replace view bench_errors as
with matches as (
    select run_id, source_artifact,
        unnest(regexp_extract_all(content,
            '(?m)^([0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+) ERROR\[([0-9]+)\] ([^\r\n]+)',
            ['reported_time', 'error_index', 'message'])) as matched
    from bench_error_logs
)
select run_id,
    try_cast(matched.error_index as bigint) as line_number,
    try_cast(matched.reported_time as time) as reported_time,
    try_cast(matched.error_index as bigint) as error_index,
    matched.message as message,
    'completion_summary' as time_semantics,
    source_artifact
from matches;

-- Individual error occurrence times and intermediate counts are unavailable.
create or replace view bench_warning_events as
select
    cast(null as varchar) as run_id,
    cast(null as timestamptz) as occurred_at
where false;

create or replace view bench_error_counts as
select
    cast(null as varchar) as run_id,
    cast(null as bigint) as line_number,
    cast(null as double) as elapsed_s,
    cast(null as bigint) as error_count
where false;
