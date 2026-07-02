def process_scheduled_call(influxdb3_local, call_time, args=None):
    """Hourly rollup: reads from 15min_vf_agg and writes to 1h_vf_agg.
    Groups by: account_id, io_op_type, io_lat_src, instance_size
    """
    import time as time_mod

    t0 = time_mod.time()
    agg_results = influxdb3_local.query(
        "SELECT account_id, io_op_type, io_lat_src, instance_size, "
        "AVG(samples) AS samples, "
        "AVG(avg_p50) AS avg_p50, "
        "AVG(avg_p99_9) AS avg_p99_9, "
        "AVG(avg_p100) AS avg_p100, "
        "AVG(total_ios) AS total_ios, "
        "AVG(total_bytes) AS total_bytes "
        "FROM \"15min_vf_agg\" "
        "WHERE time >= now() - INTERVAL '1 hour' "
        "GROUP BY account_id, io_op_type, io_lat_src, instance_size"
    )
    t1 = time_mod.time()
    query_ms = (t1 - t0) * 1000
    agg_rows = len(agg_results) if agg_results else 0

    # Write hourly aggregated rows
    t2 = time_mod.time()
    rows_written = 0
    for row in agg_results:
        line = LineBuilder("1h_vf_agg")
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
    t3 = time_mod.time()
    write_ms = (t3 - t2) * 1000

    # Perf summary
    perf = LineBuilder("_trigger_perf_test")
    perf.tag("source", "scheduled_trigger")
    perf.tag("test", "1h_agg")
    perf.int64_field("agg_output_rows", rows_written)
    perf.float64_field("query_ms", round(query_ms, 2))
    perf.float64_field("write_ms", round(write_ms, 2))
    perf.float64_field("total_ms", round((t3 - t0) * 1000, 2))
    influxdb3_local.write(perf)

    influxdb3_local.info(
        f"1h agg: {rows_written} rows from 15min_vf_agg, "
        f"query {query_ms:.0f}ms + write {write_ms:.0f}ms = {(t3-t0)*1000:.0f}ms"
    )
