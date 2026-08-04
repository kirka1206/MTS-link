import unittest

from download_mts_link import (
    _parse_stream_selection,
    build_composite_timeline,
    choose_download_plan,
    extract_media_segments,
    extract_presentation_streams,
    extract_video_streams,
    PresentationStream,
    PresentationUpdate,
    VideoStream,
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

    def test_composite_choice_is_available_without_removing_old_selection(self):
        streams = [object(), object()]
        composite, selected = choose_download_plan(streams, "C")
        self.assertTrue(composite)
        self.assertEqual(selected, streams)

        composite, selected = choose_download_plan(streams, "1")
        self.assertFalse(composite)
        self.assertEqual(selected, [streams[0]])

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

    def test_extracts_presentation_update_as_a_separate_source(self):
        record = {
            "duration": 120.0,
            "eventLogs": [
                {
                    "module": "presentation.update",
                    "relativeTime": 5.0,
                    "data": {
                        "isActive": True,
                        "fileReference": {
                            "file": {
                                "id": 10,
                                "name": "deck.pdf",
                                "downloadUrl": "https://storage/deck.pdf",
                                "slides": [{"name": "slide-1"}, {"name": "slide-2"}],
                            },
                            "slide": {"name": "slide-1"},
                        },
                    },
                },
                {
                    "module": "presentation.update",
                    "relativeTime": 50.0,
                    "data": {
                        "isActive": False,
                        "fileReference": {
                            "file": {
                                "id": 10,
                                "name": "deck.pdf",
                                "downloadUrl": "https://storage/deck.pdf",
                                "slides": [{"name": "slide-1"}, {"name": "slide-2"}],
                            },
                            "slide": {"name": "slide-2"},
                        },
                    },
                },
            ],
        }

        presentations = extract_presentation_streams(record, total_duration=120.0)

        self.assertEqual(len(presentations), 1)
        self.assertEqual(presentations[0].key, "presentation")
        self.assertEqual(presentations[0].source_url, "https://storage/deck.pdf")
        self.assertEqual(presentations[0].start_time, 5.0)
        self.assertEqual(presentations[0].duration, 45.0)
        self.assertEqual(presentations[0].slide_count, 2)

    def test_composite_timeline_uses_presentation_and_screen_share_order(self):
        presentation = PresentationStream(
            key="presentation",
            title="Демонстрация презентации",
            file_name="deck.pdf",
            source_url="https://storage/deck.pdf",
            start_time=3.0,
            duration=5.0,
            slide_count=1,
            updates=[
                PresentationUpdate(
                    relative_time=3.0,
                    is_active=True,
                    image_url="https://storage/slide.jpg",
                ),
                PresentationUpdate(relative_time=8.0, is_active=False),
            ],
        )
        screen = VideoStream(
            key="screen-share",
            title="Расшаренный экран",
            segments=[],
            duration=5.0,
            start_time=10.0,
        )

        timeline = build_composite_timeline(20.0, [presentation], screen)

        self.assertEqual(
            [(item.kind, item.start_time, item.duration) for item in timeline],
            [
                ("speaker", 0.0, 3.0),
                ("presentation", 3.0, 5.0),
                ("speaker", 8.0, 2.0),
                ("screen", 10.0, 5.0),
                ("speaker", 15.0, 5.0),
            ],
        )


if __name__ == "__main__":
    unittest.main()
