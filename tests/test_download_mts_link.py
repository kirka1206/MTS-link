import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from download_mts_link import (
    _add_missing_streams,
    _concat_segments,
    _parse_stream_selection,
    _render_speaker_segment,
    build_composite_timeline,
    choose_download_plan,
    CompositeSegment,
    DownloadError,
    extract_audio_streams,
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

    def test_parse_recording_page_accepts_alphanumeric_event_id(self):
        page = parse_recording_page(
            "https://my.mts-link.ru/j/Flant/iot_1007/record-new/19856070522"
        )
        self.assertEqual(page.session_id, 19856070522)
        self.assertIn("/api/eventsessions/19856070522/record", page.api_url)

    def test_parse_stream_selection(self):
        self.assertEqual(_parse_stream_selection("all", 3), [0, 1, 2])
        self.assertEqual(_parse_stream_selection("1,3", 3), [0, 2])
        self.assertEqual(_parse_stream_selection("1-2", 3), [0, 1])

    def test_composite_choice_is_available_without_removing_old_selection(self):
        streams = [object(), object()]
        composite, selected = choose_download_plan(streams, "C")
        self.assertEqual(composite, "composite")
        self.assertEqual(selected, streams)

        composite, selected = choose_download_plan(streams, "1")
        self.assertEqual(composite, "separate")
        self.assertEqual(selected, [streams[0]])

        package, selected = choose_download_plan(streams, "D")
        self.assertEqual(package, "full-package")
        self.assertEqual(selected, streams)

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

    def test_extracts_snapshot_only_segments_without_overlapping_files(self):
        """Повторные snapshots без mediasession.add должны покрывать запись целиком."""

        def snapshot(relative_time, speaker_url, screen_url):
            return {
                "relativeTime": relative_time,
                "snapshot": {
                    "data": {
                        "mediasession": [
                            {
                                "url": speaker_url,
                                "stream": {"conference": {"id": 10}},
                            },
                            {
                                "url": screen_url,
                                "stream": {"screensharing": {"id": 20}},
                            },
                        ]
                    }
                },
            }

        record = {
            "duration": 100.0,
            "eventLogs": [
                snapshot(0.0, "https://storage/short-start.mp4", "https://storage/screen-1.mp4"),
                snapshot(0.0, "https://storage/speaker-1.mp4", "https://storage/screen-1.mp4"),
                snapshot(40.0, "https://storage/speaker-2.mp4", "https://storage/screen-2.mp4"),
            ],
        }

        streams, duration = extract_video_streams(record)

        self.assertEqual(duration, 100.0)
        speaker = next(stream for stream in streams if stream.key == "speaker")
        screen = next(stream for stream in streams if stream.key == "screen-share")
        self.assertEqual(
            [segment.source_url for segment in speaker.segments],
            ["https://storage/speaker-1.mp4", "https://storage/speaker-2.mp4"],
        )
        self.assertEqual(
            [segment.source_url for segment in screen.segments],
            ["https://storage/screen-1.mp4", "https://storage/screen-2.mp4"],
        )
        self.assertEqual(
            [(segment.relative_time, segment.max_duration) for segment in speaker.segments],
            [(0.0, 40.0), (40.0, 60.0)],
        )
        self.assertEqual(
            [(segment.relative_time, segment.max_duration) for segment in screen.segments],
            [(0.0, 40.0), (40.0, 60.0)],
        )

    def test_concat_falls_back_to_reencoding_after_copy_error(self):
        with TemporaryDirectory() as temp_name:
            temp_dir = Path(temp_name)
            segments = [temp_dir / "one.mp4", temp_dir / "two.mp4"]
            for segment in segments:
                segment.write_bytes(b"placeholder")
            destination = temp_dir / "combined.mp4"

            with patch(
                "download_mts_link._run_ffmpeg",
                side_effect=[DownloadError("copy failed"), None, None, None],
            ) as run_ffmpeg, patch(
                "download_mts_link._probe_stream_types", return_value={"video"}
            ):
                _concat_segments(segments, destination, duration=120.0)

            self.assertEqual(run_ffmpeg.call_count, 4)
            fallback_args = run_ffmpeg.call_args_list[1].args[0]
            self.assertIn("libx264", fallback_args)
            self.assertTrue(
                any("setpts=PTS-STARTPTS" in str(item) for item in fallback_args)
            )

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

    def test_extracts_audio_only_participant_as_separate_stream(self):
        record = {
            "duration": 120.0,
            "eventLogs": [
                {
                    "relativeTime": 1.0,
                    "snapshot": {
                        "data": {
                            "mediasession": [
                                {
                                    "url": "https://storage/speaker-initial.mp4",
                                    "stream": {
                                        "conference": {"id": 10},
                                    },
                                }
                            ]
                        }
                    },
                },
                {
                    "module": "conference.update",
                    "relativeTime": 50.0,
                    "data": {
                        "id": 20,
                        "hasVideo": False,
                        "hasAudio": True,
                        "user": {"nickname": "Слушатель 1"},
                    },
                },
                {
                    "module": "mediasession.add",
                    "relativeTime": 5.0,
                    "data": {
                        "url": "https://storage/speaker-next.mp4",
                        "stream": {"conference": {"id": 10}},
                    },
                },
                {
                    "module": "mediasession.add",
                    "relativeTime": 50.0,
                    "data": {
                        "url": "https://storage/listener-question.mp4",
                        "stream": {"conference": {"id": 20}},
                    },
                },
            ],
        }

        audio_streams, duration = extract_audio_streams(record)

        self.assertEqual(duration, 120.0)
        self.assertEqual(len(audio_streams), 1)
        self.assertEqual(audio_streams[0].title, "Аудио участника: Слушатель 1")
        self.assertEqual(audio_streams[0].start_time, 50.0)
        self.assertEqual(
            audio_streams[0].segments[0].source_url,
            "https://storage/listener-question.mp4",
        )

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

    def test_audio_only_segment_gets_black_video_track(self):
        stream = VideoStream(
            key="speaker",
            title="Спикер / камера",
            segments=[],
            duration=30.0,
            start_time=0.0,
            width=1280,
            height=720,
            has_audio=True,
        )
        source = Path("audio-only.mp4")
        destination = Path("audio-only-prepared.mp4")

        # Здесь не запускаем ffmpeg: проверяем именно выбор ветки и параметры
        # восстановления. Реальный аудио-only сегмент дополнительно проверен
        # вручную на URL записи из пользовательского журнала.
        with patch("download_mts_link._probe_stream_types", return_value={"audio"}), \
             patch("download_mts_link._probe_duration", return_value=30.0), \
             patch("download_mts_link._run_ffmpeg") as run_ffmpeg:
            result = _add_missing_streams(source, destination, stream)

        self.assertEqual(result, destination)
        args = run_ffmpeg.call_args.args[0]
        self.assertIn("color=c=black:s=1280x720:r=30:d=30.000", args)
        self.assertIn("1:a:0", args)

    def test_composite_seeks_global_mixed_audio_with_video_segment(self):
        segment = CompositeSegment(kind="speaker", start_time=75.0, duration=5.0)

        with patch("download_mts_link._run_ffmpeg") as run_ffmpeg:
            _render_speaker_segment(
                Path("speaker.mp4"),
                segment,
                Path("rendered.mp4"),
                audio_path=Path("mixed-audio.m4a"),
            )

        args = run_ffmpeg.call_args.args[0]
        # Один seek относится к видео камеры, второй — к глобальной дорожке
        # mixed-аудио. Оба должны начинаться с позиции текущего участка.
        self.assertEqual(args.count("-ss"), 2)
        self.assertEqual(args[args.index("-ss") + 1], "75.000")
        second_seek = args.index("-ss", args.index("-ss") + 1)
        self.assertEqual(args[second_seek + 1], "75.000")


if __name__ == "__main__":
    unittest.main()
