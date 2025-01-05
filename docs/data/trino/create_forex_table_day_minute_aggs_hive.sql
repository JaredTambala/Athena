create table forex_all_minute_aggs
(
    ticker varchar,
    volume bigint,
    open double,
    close double,
    high double,
    low double,
    window_start_ns bigint,
    transactions bigint,
    window_start varchar,
    year varchar
    with
    (
        partition_projection_type='integer',
        partition_projection_range=array['1970','3000'],
        partition_projection_interval=1
    ),
    month varchar 
    with
    (
        partition_projection_type='integer',
        partition_projection_range=array['1','12'],
        partition_projection_interval=1,
        partition_projection_digits=2
    ),
    day varchar 
    with
    (
        partition_projection_type='integer',
        partition_projection_range=array['1','31'],
        partition_projection_interval=1,
        partition_projection_digits=2
    )
)
with
(
    external_location ='s3a://polygon/unzipped/global_forex/minute_aggs_v1/',
    format='PARQUET',
    partitioned_by=array['year', 'month', 'day'],
    partition_projection_enabled=true,
    partition_projection_location_template='s3a://polygon/unzipped/global_forex/minute_aggs_v1/year=${year}/month=${month}/day=${day}'
);