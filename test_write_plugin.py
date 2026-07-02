def process_scheduled_call(influxdb3_local, call_time, args=None):
    """Query last 15 mins, measure time, write single summary row."""
    import time as time_mod

    # Query aggregation on last 15 min
    t0 = time_mod.time()
    agg_results = influxdb3_local.query(
        "SELECT account_id, io_op_type, io_lat_src, instance_size, "
        "COUNT(*) AS samples, "
        "AVG(io_latency_p50) AS avg_p50, "
        "AVG(io_latency_p99_9) AS avg_p99_9, "
        "AVG(io_latency_p100) AS avg_p100, "
        "SUM(io_count) AS total_ios, "
        "SUM(byte_count) AS total_bytes "
        "FROM cordite_vf_latency "
        "WHERE time >= now() - INTERVAL '15 minutes' "
        "AND account_id IS NOT NULL "
        "GROUP BY account_id, io_op_type, io_lat_src, instance_size"
    )
    t1 = time_mod.time()
    query_ms = (t1 - t0) * 1000
    row_count = len(agg_results) if agg_results else 0

    # Write single summary row
    line = LineBuilder("_trigger_perf_test")
    line.tag("source", "scheduled_trigger")
    line.tag("test", "15min_query_only")
    line.int64_field("agg_output_rows", row_count)
    line.float64_field("query_ms", round(query_ms, 2))
    influxdb3_local.write(line)

    influxdb3_local.info(
        f"15min query: {row_count} agg rows returned in {query_ms:.0f}ms"
    )
