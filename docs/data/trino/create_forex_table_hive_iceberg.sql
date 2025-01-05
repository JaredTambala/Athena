create table forex_all_day_agg
(
    ticker varchar,
    volume bigint,
    open double,
    close double,
    high double,
    low double,
    window_start_ns bigint,
    transactions bigint,
    window_start varchar

)
with
(
    location ='s3a://polygon/unzipped/global_forex/day_aggs_v1/',
    format='PARQUET',
    partitioning=array['ticker']
);