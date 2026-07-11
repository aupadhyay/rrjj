import io
import unittest
from pathlib import Path

from bench.latency_runner import (
    SequenceError,
    binary_display_value,
    check_sequence,
    find_match,
    host_metadata,
    parse_rfc3339_ns,
    parse_sse,
    percentile,
    scope_watcher_estimates,
    statistics,
)


class SseParsingTest(unittest.TestCase):
    def test_parses_comments_crlf_and_multiline_data(self) -> None:
        messages = list(
            parse_sse(
                io.BytesIO(
                    b": keepalive\r\n"
                    b"id: 7\r\n"
                    b"event: event\r\n"
                    b"data: {\"a\":\r\n"
                    b"data: 1}\r\n\r\n"
                )
            )
        )
        self.assertEqual(
            messages,
            [{"event": "event", "id": "7", "data": '{"a":\n1}'}],
        )

    def test_flushes_final_message_at_eof(self) -> None:
        self.assertEqual(
            list(parse_sse(io.BytesIO(b"data: last")))[0]["data"], "last"
        )


class StatisticsTest(unittest.TestCase):
    def test_percentiles_and_summary(self) -> None:
        values = [4.0, 1.0, 3.0, 2.0]
        self.assertEqual(percentile(values, 50), 2.5)
        summary = statistics(values)
        self.assertEqual(summary["min"], 1.0)
        self.assertEqual(summary["max"], 4.0)
        self.assertEqual(summary["count"], 4)
        self.assertIsNone(percentile([], 95))


class ResultPrivacyTest(unittest.TestCase):
    def test_binary_display_value_omits_absolute_path(self) -> None:
        binary = Path("/private/worktrees/project/target/release/rrjj")
        self.assertEqual(binary_display_value(binary), "rrjj")

    def test_host_metadata_omits_hostname(self) -> None:
        metadata = host_metadata()
        self.assertNotIn("hostname", metadata)
        self.assertIn("platform", metadata)
        self.assertIn("clock_info", metadata)


class TimestampTest(unittest.TestCase):
    def test_parses_rfc3339_offsets_and_nanoseconds(self) -> None:
        self.assertEqual(
            parse_rfc3339_ns("1970-01-01T00:00:01.123456789Z"),
            1_123_456_789,
        )
        self.assertEqual(
            parse_rfc3339_ns("1970-01-01T01:00:01.123+01:00"),
            1_123_000_000,
        )

    def test_rejects_timestamp_without_offset(self) -> None:
        with self.assertRaisesRegex(ValueError, "include an offset"):
            parse_rfc3339_ns("2026-01-01T00:00:00")


class MatchingAndContinuityTest(unittest.TestCase):
    def record(self, sequence: int, event_type: str, path: str):
        key = "paths" if event_type == "touched_paths" else "changes"
        return {
            "event": {
                "v": 0,
                "seq": sequence,
                "session_id": "s",
                "type": event_type,
                "data": {key: [{"path": path}]},
            },
            "received_monotonic_ns": sequence,
            "received_wall_ns": sequence,
        }

    def test_matches_touched_path_to_later_snapshot(self) -> None:
        records = [
            self.record(4, "touched_paths", "sample.txt"),
            self.record(5, "snapshot", "other.txt"),
            self.record(6, "snapshot", "sample.txt"),
        ]
        touched, snapshot = find_match(records, "sample.txt")
        self.assertEqual(touched["event"]["seq"], 4)
        self.assertEqual(snapshot["event"]["seq"], 5)
        self.assertIsNone(find_match(records, "missing.txt"))

    def test_accepts_arbitrary_first_live_sequence_then_rejects_gap(self) -> None:
        state = {}
        check_sequence(
            {"v": 0, "seq": 9, "session_id": "s"},
            state,
        )
        check_sequence(
            {"v": 0, "seq": 10, "session_id": "s"},
            state,
        )
        with self.assertRaisesRegex(SequenceError, "expected 11"):
            check_sequence(
                {"v": 0, "seq": 12, "session_id": "s"},
                state,
            )

    def test_rejects_schema_or_session_change(self) -> None:
        with self.assertRaisesRegex(SequenceError, "schema"):
            check_sequence({"v": 1, "seq": 0, "session_id": "s"}, {})
        state = {"session": "s", "next_seq": 1}
        with self.assertRaisesRegex(SequenceError, "session changed"):
            check_sequence({"v": 0, "seq": 1, "session_id": "other"}, state)

    def test_watcher_estimate_is_counted_once_per_window(self) -> None:
        samples = [
            {
                "status": "ok",
                "edit": {"start_wall_unix_ns": started},
                "events": {"touched_paths": {"seq": 4}},
                "latency_ms": {"watcher_detection_wall_estimate": value},
            }
            for started, value in ((20, -10.0), (10, 1.0))
        ]
        scope_watcher_estimates(samples)
        self.assertNotIn(
            "watcher_detection_wall_estimate", samples[0]["latency_ms"]
        )
        self.assertTrue(
            samples[1]["events"]["watcher_detection_window_representative"]
        )


if __name__ == "__main__":
    unittest.main()
