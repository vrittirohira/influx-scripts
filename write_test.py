def process_scheduled_call(influxdb3_local, call_time, args=None):
 line = LineBuilder("trigger_write_test")
 line.tag("source", "trigger")
 line.int64_field("value", 42)
 influxdb3_local.write(line)
 influxdb3_local.info(f"Wrote 1 test row at {call_time}")
