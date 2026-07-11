import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bench.paired_runner import (
    BenchmarkDataError,
    read_json_object,
    reader_metrics,
    summarize_events,
)


class SummarizeEventsTest(unittest.TestCase):
    def write_events(self, lines: list[str]) -> Path:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        session = Path(temporary.name)
        events = session / "events"
        events.mkdir()
        (events / "000000.ndjson").write_text("\n".join(lines) + "\n")
        return session

    def test_summarizes_valid_events(self) -> None:
        session = self.write_events(
            [
                json.dumps(
                    {
                        "seq": 0,
                        "session_id": "s",
                        "type": "session_start",
                        "data": {},
                    }
                ),
                json.dumps(
                    {
                        "seq": 1,
                        "session_id": "s",
                        "type": "touched_paths",
                        "data": {
                            "raw_events": 2,
                            "paths": [
                                {
                                    "path": "a.txt",
                                    "operations": ["modify"],
                                }
                            ],
                        },
                    }
                ),
            ]
        )
        summary = summarize_events(session)
        self.assertEqual(summary["events"], 2)
        self.assertEqual(summary["distinct_touched_paths"], 1)
        self.assertEqual(summary["raw_watcher_events"], 2)

    def test_reports_malformed_event_with_line(self) -> None:
        session = self.write_events(
            [
                '{"seq":0,"session_id":"s","type":"session_start","data":{}}',
                "not-json",
            ]
        )
        with self.assertRaisesRegex(BenchmarkDataError, r":2:"):
            summarize_events(session)

    def test_rejects_missing_session_and_sequence_gap(self) -> None:
        missing_session = self.write_events(
            ['{"seq":0,"type":"session_start","data":{}}']
        )
        with self.assertRaisesRegex(BenchmarkDataError, "missing event session_id"):
            summarize_events(missing_session)

        gap = self.write_events(
            [
                '{"seq":0,"session_id":"s","type":"session_start","data":{}}',
                '{"seq":2,"session_id":"s","type":"flush","data":{}}',
            ]
        )
        with self.assertRaisesRegex(BenchmarkDataError, "expected 1"):
            summarize_events(gap)

    def test_reports_missing_and_malformed_session_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            manifest = Path(temporary) / "manifest.json"
            with self.assertRaisesRegex(BenchmarkDataError, "missing manifest"):
                read_json_object(manifest, "manifest")
            manifest.write_text("{")
            with self.assertRaisesRegex(BenchmarkDataError, "malformed manifest"):
                read_json_object(manifest, "manifest")

    @patch("bench.paired_runner.run_json")
    def test_rejects_missing_durable_operation(self, run_json_mock) -> None:
        run_json_mock.return_value = ([{"op": "op:other", "tree": "t:1"}], 0.1)
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(BenchmarkDataError, "op:durable.*missing"):
                reader_metrics(
                    Path("rrjj"),
                    Path(temporary) / "session",
                    "op:durable",
                    Path(temporary),
                )


if __name__ == "__main__":
    unittest.main()
