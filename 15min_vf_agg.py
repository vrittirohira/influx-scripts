def process_scheduled_call(influxdb3_local, call_time, args=None):
    """Every 15 mins: aggregate raw data and write to 15min_vf_agg.
    Groups by: account_id, io_op_type, io_lat_src, instance_size
    """
    import time as time_mod

    # Count raw rows
    t0 = time_mod.time()
    count_results = influxdb3_local.query(
        "SELECT COUNT(*) AS n FROM cordite_vf_latency "
        "WHERE time >= now() - INTERVAL '15 minutes'"
    )
    t1 = time_mod.time()
    count_ms = (t1 - t0) * 1000
    raw_rows = count_results[0]["n"] if count_results else 0

    # Run aggregation
    t2 = time_mod.time()
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
    t3 = time_mod.time()
    agg_ms = (t3 - t2) * 1000
    agg_rows = len(agg_results) if agg_results else 0

    # Write aggregated data rows to 15min_vf_agg
    t4 = time_mod.time()
    rows_written = 0
    for row in agg_results:
        line = LineBuilder("15min_vf_agg")
        if row.get("account_id"):
            line.tag("account_id", str(row["account_id"]))
        if row.get("io_op_type"):
            line.tag("io_op_type", str(row["io_op_type"]))
        if row.get("io_lat_src"):
            line.tag("io_lat_src", str(row["io_lat_src"]))
        if row.get("instance_size"):
            line.tag("instance_size", str(row["instance_size"]))
        if row.get("samples") is not None:
            line.int64_field("samples", int(row["samples"]))
        if row.get("avg_p50") is not None:
            line.float64_field("avg_p50", float(row["avg_p50"]))
        if row.get("avg_p99_9") is not None:
            line.float64_field("avg_p99_9", float(row["avg_p99_9"]))
        if row.get("avg_p100") is not None:
            line.float64_field("avg_p100", float(row["avg_p100"]))
        if row.get("total_ios") is not None:
            line.int64_field("total_ios", int(row["total_ios"]))
        if row.get("total_bytes") is not None:
            line.int64_field("total_bytes", int(row["total_bytes"]))
        influxdb3_local.write(line)
        rows_written += 1
    t5 = time_mod.time()
    write_ms = (t5 - t4) * 1000

    # Write perf summary to separate table
    perf = LineBuilder("_trigger_perf_test")
    perf.tag("source", "scheduled_trigger")
    perf.tag("test", "15min_no_iosize")
    perf.int64_field("raw_row_count", int(raw_rows))
    perf.float64_field("count_query_ms", round(count_ms, 2))
    perf.int64_field("agg_output_rows", rows_written)
    perf.float64_field("agg_query_ms", round(agg_ms, 2))
    perf.float64_field("write_ms", round(write_ms, 2))
    perf.float64_field("total_ms", round((t5 - t0) * 1000, 2))
    influxdb3_local.write(perf)

    influxdb3_local.info(
        f"15min: {raw_rows} raw -> {rows_written} agg rows, "
        f"query {agg_ms:.0f}ms + write {write_ms:.0f}ms = {(t5-t0)*1000:.0f}ms"
    )
