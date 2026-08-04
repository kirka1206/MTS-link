import unittest

from download_mts_link import (
    _parse_stream_selection,
    extract_media_segments,
    extract_video_streams,
    parse_recording_page,
)


class RecordingParsingTests(unittest.TestCase):
    def test_parse_recording_page(self):
        page = parse_recording_page(
            "https://my.mts-link.ru/j/Deckhouse/19443161368/record-new/18659320070"
        )
        self.assertEqual(page.session_id, 18659320070)
        self.assertIn("/api/eventsessions/18659320070/record", page.api_url)

    def test_parse_stream_selection(self):
        self.assertEqual(_parse_stream_selection("all", 3), [0, 1, 2])
        self.assertEqual(_parse_stream_selection("1,3", 3), [0, 2])
        self.assertEqual(_parse_stream_selection("1-2", 3), [0, 1])

    def test_extracts_main_segments_and_trims_initial_preroll(self):
        record = {
            "duration": 120.0,
            "eventLogs": [
                {
                    "relativeTime": 10.0,
                    "snapshot": {
                        "data": {
                            "mediasession": [
                                {
                                    "url": "https://storage/initial.mp4",
                                    "hlsUrl": "https://delivery/initial/playlist.m3u8",
                                    "stream": {"conference": {"id": 1}},
                                }
                            ]
                        }
                    },
                },
                {
                    "module": "mediasession.add",
                    "relativeTime": 10.0,
                    "data": {
                        "url": "https://storage/main.mp4",
                        "hlsUrl": "https://delivery/main/playlist.m3u8",
                        "stream": {"conference": {"id": 1}},
                    },
                },
                {
                    "module": "mediasession.add",
                    "relativeTime": 100.0,
                    "data": {
                        "url": "https://storage/screen.mp4",
                        "hlsUrl": "https://delivery/screen/playlist.m3u8",
                        "stream": {"screensharing": {"id": 2}},
                    },
                },
                {
                    "module": "mediasession.add",
                    "relativeTime": 100.0,
                    "data": {
                        "url": "https://storage/final.mp4",
                        "hlsUrl": "https://delivery/final/playlist.m3u8",
                        "stream": {"conference": {"id": 3}},
                    },
                },
            ],
        }

        segments, duration = extract_media_segments(record)

        self.assertEqual(duration, 120.0)
        self.assertEqual(
            [segment.source_url for segment in segments],
            [
                "https://storage/initial.mp4",
                "https://storage/main.mp4",
                "https://storage/final.mp4",
            ],
        )
        self.assertEqual(segments[0].trim_duration, 10.0)

        streams, _ = extract_video_streams(record)
        self.assertEqual([stream.key for stream in streams], ["speaker", "screen-share"])
        self.assertEqual(streams[1].start_time, 100.0)
        self.assertEqual(streams[1].segments[0].source_url, "https://storage/screen.mp4")


if __name__ == "__main__":
    unittest.main()
