#!/usr/bin/env python3
"""Download a complete recording from an MTS Link public recording page."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


LOG = logging.getLogger("mts-link-downloader")
API_HOST = "https://gw.mts-link.ru"
RECORD_PATH_RE = re.compile(
    r"^/j/(?P<organization>[^/]+)/(?P<event_id>\d+)/"
    r"record-new/(?P<session_id>\d+)/?$"
)


class DownloadError(RuntimeError):
    """A user-facing download error."""


@dataclass
class RecordingPage:
    url: str
    session_id: int
    api_url: str


@dataclass
class MediaSegment:
    source_url: str
    hls_url: Optional[str]
    relative_time: float
    initial: bool = False
    trim_duration: Optional[float] = None


@dataclass
class VideoStream:
    key: str
    title: str
    segments: List[MediaSegment]
    duration: float
    start_time: float
    codec: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    has_audio: bool = False

    @property
    def end_time(self) -> float:
        return self.start_time + self.duration


def parse_recording_page(url: str) -> RecordingPage:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise DownloadError("URL должен начинаться с http:// или https://.")

    match = RECORD_PATH_RE.match(parsed.path)
    if not match or parsed.netloc not in {"my.mts-link.ru", "mts-link.ru"}:
        raise DownloadError(
            "Поддерживаются ссылки на записи МТС Линк вида "
            "https://my.mts-link.ru/j/<организация>/<event_id>/record-new/<session_id>."
        )

    session_id = int(match.group("session_id"))
    return RecordingPage(
        url=url,
        session_id=session_id,
        api_url=f"{API_HOST}/api/eventsessions/{session_id}/record?withoutCuts=false",
    )


def _stream_key(value: Any) -> Optional[str]:
    if not isinstance(value, dict):
        return None
    stream = value.get("stream") or {}
    if not isinstance(stream, dict):
        return None
    if stream.get("screensharing"):
        return "screen-share"
    if stream.get("conference"):
        return "speaker"
    return None


def _media_segment(
    value: Dict[str, Any], relative_time: float, initial: bool
) -> Optional[MediaSegment]:
    if not _stream_key(value):
        return None

    source_url = value.get("url")
    hls_url = value.get("hlsUrl")
    if not isinstance(source_url, str) or not source_url.startswith(("http://", "https://")):
        source_url = ""
    if not isinstance(hls_url, str) or not hls_url.startswith(("http://", "https://")):
        hls_url = None
    if not source_url and not hls_url:
        return None

    return MediaSegment(
        source_url=source_url,
        hls_url=hls_url,
        relative_time=max(0.0, float(relative_time)),
        initial=initial,
    )


def extract_video_streams(record: Dict[str, Any]) -> Tuple[List[VideoStream], float]:
    """Extract logical video streams and their sequential media files.

    MTS Link stores a recording as several media sessions. The first cut may
    contain a short pre-roll in its source file. Later cut snapshots describe
    active live streams, so only their actual ``mediasession.add`` events are
    appended after the first snapshot. Conference media is grouped as the
    speaker/camera stream, while screen sharing is kept as its own stream.
    """

    event_logs = record.get("eventLogs") or []
    if not isinstance(event_logs, list):
        raise DownloadError("Ответ МТС Линк содержит некорректный журнал записи.")

    initial_by_key: Dict[str, MediaSegment] = {}
    for event in event_logs:
        if not isinstance(event, dict):
            continue
        snapshot = event.get("snapshot") or {}
        data = snapshot.get("data") if isinstance(snapshot, dict) else None
        sessions = data.get("mediasession") if isinstance(data, dict) else None
        if not isinstance(sessions, list):
            continue
        for session in sessions:
            candidate = _media_segment(session, 0.0, initial=True)
            key = _stream_key(session)
            if candidate and key and key not in initial_by_key:
                initial_by_key[key] = candidate

    additions_by_key: Dict[str, List[MediaSegment]] = {}
    for event in event_logs:
        if not isinstance(event, dict) or event.get("module") != "mediasession.add":
            continue
        data = event.get("data")
        if not isinstance(data, dict):
            continue
        relative_time = event.get("relativeTime", 0.0)
        try:
            relative_time = float(relative_time)
        except (TypeError, ValueError):
            continue
        candidate = _media_segment(data, relative_time, initial=False)
        key = _stream_key(data)
        if candidate and key:
            additions_by_key.setdefault(key, []).append(candidate)

    stream_names = {
        "speaker": ("Спикер / камера", True),
        "screen-share": ("Расшаренный экран", False),
    }
    streams: List[VideoStream] = []
    for key, (title, has_audio) in stream_names.items():
        additions = sorted(additions_by_key.get(key, []), key=lambda item: item.relative_time)
        initial = initial_by_key.get(key)
        segments: List[MediaSegment] = []
        seen: set = set()
        if initial:
            initial_source = initial.source_url or initial.hls_url
            if initial_source:
                segments.append(initial)
                seen.add(initial_source)

        for candidate in additions:
            candidate_source = candidate.source_url or candidate.hls_url
            if not candidate_source or candidate_source in seen:
                continue
            segments.append(candidate)
            seen.add(candidate_source)

        if initial and additions and segments:
            # The first file can contain pre-roll before the recording starts.
            segments[0].trim_duration = max(0.0, additions[0].relative_time)

        if not segments:
            continue

        start_time = 0.0 if initial else segments[0].relative_time
        streams.append(
            VideoStream(
                key=key,
                title=title,
                segments=segments,
                duration=0.0,
                start_time=start_time,
                has_audio=has_audio,
            )
        )

    if not streams:
        raise DownloadError(
            "На странице не найден ни один видеопоток. "
            "Возможно, запись удалена или доступ к ней ограничен."
        )

    try:
        total_duration = float(record.get("duration") or 0.0)
    except (TypeError, ValueError):
        total_duration = 0.0

    for stream in streams:
        if stream.key == "speaker":
            stream.duration = max(0.0, total_duration)

    return streams, max(0.0, total_duration)


def extract_media_segments(record: Dict[str, Any]) -> Tuple[List[MediaSegment], float]:
    """Backward-compatible helper returning the speaker stream only."""

    streams, duration = extract_video_streams(record)
    speaker = next((stream for stream in streams if stream.key == "speaker"), None)
    if speaker:
        return speaker.segments, duration
    return streams[0].segments, duration


def _format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _safe_filename(name: str, fallback: str) -> str:
    name = re.sub(r"[\\/:*?\"<>|\x00-\x1f]", "_", name).strip(" .")
    name = re.sub(r"\s+", " ", name)
    return (name or fallback)[:180] + ".mp4"


def _probe_remote_media(url: str) -> Dict[str, Any]:
    """Read lightweight codec/size/duration metadata without downloading media."""

    completed = subprocess.run(
        [
            _ffprobe_path(),
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=codec_name,width,height:format=duration",
            "-of",
            "json",
            url,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return {}
    try:
        data = json.loads(completed.stdout)
    except json.JSONDecodeError:
        return {}
    stream = (data.get("streams") or [{}])[0]
    fmt = data.get("format") or {}
    result = dict(stream) if isinstance(stream, dict) else {}
    try:
        result["duration"] = float(fmt.get("duration"))
    except (TypeError, ValueError):
        pass
    return result


def enrich_video_streams(streams: Sequence[VideoStream], total_duration: float) -> None:
    """Add codec, resolution and duration data used by the interactive catalog."""

    for stream in streams:
        first_url = stream.segments[0].source_url or stream.segments[0].hls_url
        if first_url:
            metadata = _probe_remote_media(first_url)
            stream.codec = metadata.get("codec_name")
            stream.width = metadata.get("width")
            stream.height = metadata.get("height")

        if stream.key == "speaker":
            stream.duration = total_duration
            continue

        segment_duration = 0.0
        for segment in stream.segments:
            source_url = segment.source_url or segment.hls_url
            if not source_url:
                continue
            metadata = _probe_remote_media(source_url)
            duration = metadata.get("duration")
            if isinstance(duration, (int, float)):
                segment_duration += float(duration)
        if stream.segments and stream.segments[0].trim_duration is not None:
            segment_duration = min(segment_duration, stream.segments[0].trim_duration)
        stream.duration = max(0.0, segment_duration)


def print_stream_catalog(streams: Sequence[VideoStream], total_duration: float) -> None:
    print("Доступные видеопотоки:", flush=True)
    for index, stream in enumerate(streams, start=1):
        codec = stream.codec or "не определён"
        size = (
            f"{stream.width}x{stream.height}"
            if stream.width and stream.height
            else "размер неизвестен"
        )
        audio = "видео + звук" if stream.has_audio else "только видео"
        duration = _format_duration(stream.duration) if stream.duration else "не определена"
        interval_end = stream.end_time if stream.duration else stream.start_time
        interval = f"{_format_duration(stream.start_time)}–{_format_duration(interval_end)}"
        if stream.key == "speaker" and total_duration:
            interval = f"00:00:00–{_format_duration(total_duration)}"
        print(f"\n{index}. {stream.title}", flush=True)
        print(f"   формат: {codec}, {size}, {audio}", flush=True)
        print(
            f"   частей: {len(stream.segments)}, активная длительность: {duration}",
            flush=True,
        )
        print(f"   положение в записи: {interval}", flush=True)
    print("\nA. Все потоки", flush=True)


def _parse_stream_selection(value: str, stream_count: int) -> List[int]:
    normalized = value.strip().lower()
    if normalized in {"", "a", "all", "все", "*"}:
        return list(range(stream_count))

    selected: List[int] = []
    for item in normalized.split(","):
        item = item.strip()
        if "-" in item:
            start_text, end_text = item.split("-", 1)
            start, end = int(start_text), int(end_text)
            selected.extend(range(start - 1, end))
        else:
            selected.append(int(item) - 1)
    if not selected or any(index < 0 or index >= stream_count for index in selected):
        raise ValueError
    return list(dict.fromkeys(selected))


def choose_video_streams(
    streams: Sequence[VideoStream], requested: Optional[str]
) -> List[VideoStream]:
    if requested is not None:
        try:
            indexes = _parse_stream_selection(requested, len(streams))
        except (TypeError, ValueError):
            raise DownloadError("Некорректный --streams. Используйте all или номера, например 1,2.")
        return [streams[index] for index in indexes]

    if not sys.stdin.isatty():
        LOG.info("Интерактивный выбор недоступен, выбираю все потоки.")
        return list(streams)

    while True:
        try:
            value = input("\nВведите A для всех потоков или номера через запятую: ")
            indexes = _parse_stream_selection(value, len(streams))
            return [streams[index] for index in indexes]
        except (TypeError, ValueError):
            print("Не удалось распознать выбор. Пример: A или 1,2.")


def _ffmpeg_path() -> str:
    path = shutil.which("ffmpeg")
    if not path:
        raise DownloadError(
            "Не найден ffmpeg. Установите его отдельно, например на macOS: "
            "brew install ffmpeg."
        )
    return path


def _ffprobe_path() -> str:
    path = shutil.which("ffprobe")
    if not path:
        raise DownloadError(
            "Не найден ffprobe. Обычно он устанавливается вместе с ffmpeg."
        )
    return path


def _run_ffmpeg(args: Sequence[str], description: str) -> None:
    command = [_ffmpeg_path(), "-hide_banner", "-nostdin", "-loglevel", "warning"]
    command.extend(args)
    LOG.info(description)
    completed = subprocess.run(command, stdout=subprocess.DEVNULL)
    if completed.returncode != 0:
        raise DownloadError(f"ffmpeg не смог выполнить операцию: {description}")


def _probe_duration(path: Path) -> float:
    completed = subprocess.run(
        [
            _ffprobe_path(),
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=nw=1:nk=1",
            str(path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise DownloadError(f"Не удалось определить длительность файла {path.name}.")
    try:
        return float(completed.stdout.strip())
    except ValueError as exc:
        raise DownloadError(f"ffprobe вернул некорректную длительность для {path.name}.") from exc


def _download_source(segment: MediaSegment, destination: Path, referer: str) -> None:
    headers = f"Referer: {referer}\r\nOrigin: {urlparse(referer).scheme}://{urlparse(referer).netloc}\r\n"
    candidates = [url for url in (segment.source_url, segment.hls_url) if url]
    last_error: Optional[Exception] = None

    for source_url in candidates:
        try:
            destination.unlink(missing_ok=True)
            _run_ffmpeg(
                [
                    "-headers",
                    headers,
                    "-i",
                    source_url,
                    "-map",
                    "0:v:0",
                    "-map",
                    "0:a:0?",
                    "-c",
                    "copy",
                    "-movflags",
                    "+faststart",
                    "-y",
                    str(destination),
                ],
                f"Скачивание источника {source_url.rsplit('/', 1)[-1]}",
            )
            if destination.exists() and destination.stat().st_size > 0:
                return
        except (DownloadError, OSError) as exc:
            last_error = exc
            LOG.warning("Источник не скачан, пробую запасной вариант: %s", source_url)

    if last_error:
        raise DownloadError(f"Не удалось скачать ни один источник для сегмента: {last_error}")
    raise DownloadError("Для сегмента отсутствует URL источника.")


def _trim_tail(source: Path, destination: Path, seconds: float) -> None:
    duration = _probe_duration(source)
    if seconds <= 0 or seconds >= duration - 0.25:
        shutil.copy2(source, destination)
        return

    start = max(0.0, duration - seconds)
    _run_ffmpeg(
        [
            "-i",
            str(source),
            "-ss",
            f"{start:.3f}",
            "-t",
            f"{seconds:.3f}",
            "-map",
            "0:v:0",
            "-map",
            "0:a:0?",
            "-c",
            "copy",
            "-avoid_negative_ts",
            "make_zero",
            "-y",
            str(destination),
        ],
        f"Отбрасывание преролла ({seconds:.1f} с)",
    )


def _concat_segments(segment_paths: Sequence[Path], destination: Path, duration: float) -> None:
    list_path = destination.with_suffix(".concat.txt")
    lines = []
    for path in segment_paths:
        escaped = str(path).replace("'", "'\\''")
        lines.append(f"file '{escaped}'")
    list_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    try:
        args: List[str] = [
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(list_path),
            "-map",
            "0:v:0",
            "-map",
            "0:a:0?",
            "-c",
            "copy",
            "-movflags",
            "+faststart",
        ]
        if duration > 0:
            args.extend(["-t", f"{duration:.3f}"])
        args.extend(["-y", str(destination)])
        _run_ffmpeg(args, "Объединение сегментов в итоговый MP4")
    finally:
        list_path.unlink(missing_ok=True)


async def analyze_page(page_info: RecordingPage, headed: bool) -> Dict[str, Any]:
    """Load the page, verify that it is playable, and return its record JSON."""

    payload: Dict[str, Any] = {}
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=not headed)
        context = await browser.new_context()
        page = await context.new_page()
        page.set_default_timeout(20_000)

        async def save_record_response(response: Any) -> None:
            if "/api/eventsessions/" not in response.url or "/record" not in response.url:
                return
            try:
                data = await response.json()
                if isinstance(data, dict) and data.get("eventLogs"):
                    payload.update(data)
            except Exception:
                return

        def on_response(response: Any) -> None:
            if "/api/eventsessions/" in response.url and "/record" in response.url:
                asyncio.create_task(save_record_response(response))

        page.on("response", on_response)
        try:
            LOG.info("Открываю страницу записи в браузере")
            await page.goto(page_info.url, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(2_000)

            try:
                video_count = await page.locator("video").count()
                if video_count:
                    video = page.locator("video").first
                    await video.click(position={"x": 1, "y": 1})
                    await page.wait_for_timeout(2_000)
            except Exception as exc:
                LOG.debug("Плеер не потребовал запуска или не был нажат: %s", exc)

            if not payload:
                if headed:
                    LOG.info(
                        "Если в открытом окне требуется вход, выполните его и нажмите Enter здесь."
                    )
                    await asyncio.to_thread(input, "Нажмите Enter после входа (или сразу для продолжения): ")
                    await page.reload(wait_until="domcontentloaded", timeout=60_000)
                    await page.wait_for_timeout(2_000)
                response = await context.request.get(
                    page_info.api_url,
                    headers={"Referer": page_info.url},
                    timeout=60_000,
                )
                if not response.ok:
                    raise DownloadError(
                        f"Сервис МТС Линк вернул HTTP {response.status} при чтении записи."
                    )
                data = await response.json()
                if isinstance(data, dict):
                    payload = data
        except PlaywrightTimeoutError as exc:
            raise DownloadError(
                "Страница МТС Линк не загрузилась вовремя. Проверьте ссылку и доступ к записи."
            ) from exc
        finally:
            await browser.close()

    if not payload:
        raise DownloadError("Не удалось получить описание записи со страницы МТС Линк.")
    return payload


def stream_output_path(
    output_dir: Path,
    filename: Optional[str],
    title: str,
    session_id: int,
    stream: VideoStream,
) -> Path:
    suffix = f"-{stream.key}.mp4"
    if filename:
        supplied = Path(filename)
        parent = supplied.parent if supplied.is_absolute() else output_dir
        stem = supplied.stem if supplied.suffix else supplied.name
        return parent / f"{stem}{suffix}"
    safe_title = _safe_filename(title, f"mts-link-{session_id}")[:-4]
    return output_dir / f"{safe_title}{suffix}"


def download_stream(
    page_info: RecordingPage,
    stream: VideoStream,
    output_path: Path,
    overwrite: bool,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists() and not overwrite:
        raise DownloadError(
            f"Файл уже существует: {output_path}. Добавьте --overwrite для перезаписи."
        )

    segments = stream.segments
    duration = stream.duration
    LOG.info("Поток «%s»: сегментов %d", stream.title, len(segments))
    if duration:
        LOG.info("Длительность потока: %s", _format_duration(duration))

    partial_path = output_path.with_name(output_path.name + ".part")
    partial_path.unlink(missing_ok=True)

    with tempfile.TemporaryDirectory(prefix="mts-link-") as temp_name:
        temp_dir = Path(temp_name)
        local_segments: List[Path] = []
        for index, segment in enumerate(segments, start=1):
            downloaded = temp_dir / f"source-{index:03d}.mp4"
            _download_source(segment, downloaded, page_info.url)
            if index == 1 and segment.trim_duration is not None:
                trimmed = temp_dir / "source-001-trimmed.mp4"
                _trim_tail(downloaded, trimmed, segment.trim_duration)
                local_segments.append(trimmed)
            else:
                local_segments.append(downloaded)

        combined = temp_dir / "combined.mp4"
        _concat_segments(local_segments, combined, duration)
        shutil.copy2(combined, partial_path)

    os.replace(partial_path, output_path)
    actual_duration = _probe_duration(output_path)
    LOG.info("Готово: %s (%s)", output_path, _format_duration(actual_duration))
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Скачать полную запись с публичной страницы МТС Линк."
    )
    parser.add_argument("url", help="URL страницы записи МТС Линк")
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=Path("downloads"),
        help="Папка для итогового MP4 (по умолчанию: downloads)",
    )
    parser.add_argument(
        "-f",
        "--filename",
        type=str,
        help="Имя итогового файла; расширение .mp4 добавится автоматически",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Показать окно браузера во время анализа страницы",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Перезаписать существующий итоговый файл",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Только проанализировать страницу и вывести найденные потоки",
    )
    parser.add_argument(
        "--streams",
        help="Потоки для скачивания: all или номера через запятую, например 1,2",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Показывать подробные сообщения",
    )
    return parser


async def async_main(args: argparse.Namespace) -> int:
    page_info = parse_recording_page(args.url)
    record = await analyze_page(page_info, headed=args.headed)
    streams, total_duration = extract_video_streams(record)
    enrich_video_streams(streams, total_duration)
    print_stream_catalog(streams, total_duration)
    if args.dry_run:
        return 0

    selected_streams = choose_video_streams(streams, args.streams)
    title = str(record.get("name") or f"mts-link-{page_info.session_id}")
    print(
        "\nВыбрано потоков:",
        ", ".join(stream.title for stream in selected_streams),
        flush=True,
    )
    for stream in selected_streams:
        output_path = stream_output_path(
            output_dir=args.output_dir,
            filename=args.filename,
            title=title,
            session_id=page_info.session_id,
            stream=stream,
        )
        result = download_stream(
            page_info=page_info,
            stream=stream,
            output_path=output_path,
            overwrite=args.overwrite,
        )
        print(result, flush=True)
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )
    try:
        return asyncio.run(async_main(args))
    except DownloadError as exc:
        LOG.error("%s", exc)
        return 1
    except KeyboardInterrupt:
        LOG.error("Скачивание прервано пользователем.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
