def process_scheduled_call(influxdb3_local, call_time, args=None):
    """Minimal write test using LineBuilder API."""
    line = LineBuilder("_trigger_write_test")
    line.tag("source", "trigger")
    line.tag("run_id", str(int(call_time.timestamp())))
    line.int64_field("value", 42)
    line.float64_field("latency_ms", 3.14)
    line.string_field("message", "hello from test plugin")
    influxdb3_local.write(line)
    influxdb3_local.info(f"Wrote 1 test row at {call_time}")
