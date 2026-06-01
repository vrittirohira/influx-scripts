def process_scheduled_call(influxdb3_local, call_time, args=None):
    """Hourly rollup: cordite_pf_qdepth -> cordite_pf_qdepth_hourly"""
    query = """
    SELECT
      DATE_BIN(INTERVAL '1 hour', time) AS time,
      card_model, hw_type, asset_tag, drive_sn, drive_model,
      az, instance_size, stack, usage,

      APPROX_PERCENTILE_CONT(q_depth_p50, 0.5)    AS p50_of_p50_qd,
      APPROX_PERCENTILE_CONT(q_depth_p50, 0.99)   AS p99_of_p50_qd,
      APPROX_PERCENTILE_CONT(q_depth_p99_9, 0.5)  AS p50_of_p99_9_qd,
      APPROX_PERCENTILE_CONT(q_depth_p99_9, 0.99) AS p99_of_p99_9_qd,
      APPROX_PERCENTILE_CONT(q_depth_p100, 0.99)  AS p99_of_p100_qd,
      COUNT(*) AS sample_count

    FROM cordite_pf_qdepth
    WHERE time >= NOW() - INTERVAL '2 hours'
      AND time < NOW() - INTERVAL '1 hour'
    GROUP BY 1, card_model, hw_type, asset_tag, drive_sn, drive_model,
             az, instance_size, stack, usage
    """
    results = influxdb3_local.query(query)
    for row in results:
        influxdb3_local.write_to("cordite_pf_qdepth_hourly", row)
    influxdb3_local.info(f"QD hourly rollup: {len(results)} rows written at {call_time}")
