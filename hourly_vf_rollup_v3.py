def process_scheduled_call(influxdb3_local, call_time, args=None):
 query = "SELECT DATE_BIN(INTERVAL '1 hour', time) AS time, card_model, hardware_type, asset_tag, io_op_type, stack, drive_sn, usage, drive_instance_id, card_sn, drive_fw, cordite_fw, instance_size, io_size_lo, drive_model, az, io_lat_src, io_size_hi, disk_id, APPROX_PERCENTILE_CONT(io_latency_p50, 0.99) AS p99_of_p50_lat, APPROX_PERCENTILE_CONT(io_latency_p99_9, 0.5) AS p50_of_p99_9_lat, APPROX_PERCENTILE_CONT(io_latency_p99_9, 0.99) AS p99_of_p99_9_lat, APPROX_PERCENTILE_CONT(io_latency_p100, 0.99) AS p99_of_p100_lat, CASE WHEN SUM(io_count) > 0 THEN SUM(byte_count) / SUM(io_count) ELSE 0 END AS avg_io_size, CASE WHEN COUNT(*) > 0 THEN SUM(io_count) / (60.0 * COUNT(*)) ELSE 0 END AS avg_iops, SUM(io_count) AS total_io_count, SUM(byte_count) AS total_byte_count, COUNT(*) AS sample_count FROM cordite_vf_latency WHERE time >= NOW() - INTERVAL '2 hours' AND time < NOW() - INTERVAL '1 hour' GROUP BY 1, card_model, hardware_type, asset_tag, io_op_type, stack, drive_sn, usage, drive_instance_id, card_sn, drive_fw, cordite_fw, instance_size, io_size_lo, drive_model, az, io_lat_src, io_size_hi, disk_id"
 results = influxdb3_local.query(query)
 for row in results:
  line = LineBuilder("cordite_vf_hourly")
  for k, v in row.items():
   if v is None:
    continue
   elif k == "time":
    line.time_ns(v)
   elif isinstance(v, str):
    line.tag(k, v)
   elif isinstance(v, int):
    line.int64_field(k, v)
   elif isinstance(v, float):
    line.float64_field(k, v)
  influxdb3_local.write(line)
 influxdb3_local.info(f"VF rollup: {len(results)} rows at {call_time}")
