#!/usr/bin/env python3
"""Скачивание и сборка записей с публичной страницы МТС Линк.

Скрипт работает в несколько независимых этапов:

1. ``analyze_page`` открывает страницу через Playwright и получает JSON-запись
   из API МТС Линк. Это позволяет использовать те же актуальные ссылки и
   временные метки, которые использует веб-плеер.
2. ``extract_video_streams`` разбирает ``mediasession`` и группирует файлы
   камеры/спикера и screen share. Один логический поток может состоять из
   нескольких последовательных MP4-сегментов.
3. ``extract_presentation_streams`` отдельно разбирает события
   ``presentation.update``. Презентация в МТС Линк не является H.264-потоком:
   API сообщает PDF и изображения активных слайдов.
4. Каталог позволяет сохранить каждый источник отдельно или выбрать режим
   ``C``/``--composite``. В этом режиме строится общая временная шкала,
   слайды превращаются в видеоряд, а видео и аудио спикера накладываются на
   демонстрируемые материалы.
5. Все временные файлы создаются в ``TemporaryDirectory`` и удаляются после
   завершения. Итоговый файл сначала записывается с суффиксом ``.part`` и
   переименовывается в конечное имя только после успешной сборки.

Для работы нужны Python из виртуального окружения, Playwright/Chromium и
внешние программы ``ffmpeg`` и ``ffprobe``.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


LOG = logging.getLogger("mts-link-downloader")
API_HOST = "https://gw.mts-link.ru"
RECORD_PATH_RE = re.compile(
    r"^/j/(?P<organization>[^/]+)/(?P<event_id>\d+)/"
    r"record-new/(?P<session_id>\d+)/?$"
)


class DownloadError(RuntimeError):
    """Ошибка, которую можно безопасно показать пользователю в терминале."""


@dataclass
class RecordingPage:
    """Разобранные параметры страницы записи и соответствующего API-запроса."""

    url: str
    session_id: int
    api_url: str


@dataclass
class MediaSegment:
    """Один физический видеофайл внутри логического видеопотока.

    ``relative_time`` — момент начала сегмента относительно всей записи.
    ``trim_duration`` используется только для первого файла камеры: в нём
    иногда присутствует преролл до фактического начала записи.
    """

    source_url: str
    hls_url: Optional[str]
    relative_time: float
    initial: bool = False
    trim_duration: Optional[float] = None


@dataclass
class VideoStream:
    """Логический видеопоток, собранный из одного или нескольких сегментов."""

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
        """Вернуть момент окончания активной части потока в записи."""

        return self.start_time + self.duration


@dataclass
class PresentationUpdate:
    """Одно событие смены состояния или текущего слайда презентации."""

    relative_time: float
    is_active: bool
    image_url: Optional[str] = None
    slide_name: Optional[str] = None


@dataclass
class PresentationStream:
    """Презентационный источник, описанный событиями, а не видеофайлом.

    ``source_url`` ведёт на исходный PDF для отдельного скачивания.
    ``updates`` содержит URL изображений слайдов и временные метки, поэтому
    эти же данные можно использовать для формирования сводного MP4.
    """

    key: str
    title: str
    file_name: str
    source_url: str
    start_time: float
    duration: float
    slide_count: int
    updates: List[PresentationUpdate] = field(default_factory=list)

    @property
    def end_time(self) -> float:
        """Вернуть правую границу общего интервала показа презентации."""

        return self.start_time + self.duration


@dataclass
class CompositeSegment:
    """Участок итогового ролика с одним типом основного изображения.

    ``kind`` принимает значения ``speaker``, ``presentation`` или ``screen``.
    Для презентации ``image_url`` указывает на конкретный слайд; для остальных
    типов он не используется.
    """

    kind: str
    start_time: float
    duration: float
    image_url: Optional[str] = None


SelectableStream = Union[VideoStream, PresentationStream]
DOWNLOAD_SEPARATE = "separate"
DOWNLOAD_COMPOSITE = "composite"
DOWNLOAD_FULL_PACKAGE = "full-package"


def parse_recording_page(url: str) -> RecordingPage:
    """Проверить URL записи и построить адрес API с её описанием.

    Здесь намеренно поддерживается только публичный формат ``/j/...``.
    Идентификатор сессии берётся из последнего сегмента URL; организация и
    ``event_id`` нужны для проверки формата, но в API-запросе не используются.
    """

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
    """Определить тип media-session по флагам из JSON МТС Линк.

    В API камера/микрофон обозначены ключом ``conference``, а экранная
    демонстрация — ключом ``screensharing``. Неизвестные типы пропускаются,
    чтобы не ошибочно принять служебный ресурс за видеопоток.
    """

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
    """Преобразовать одну запись ``mediasession`` в безопасную модель.

    У сегмента может быть прямой MP4 URL, HLS URL или оба варианта. Прямой
    MP4 используется первым, а HLS остаётся резервным вариантом для ffmpeg.
    """

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

    # В журнале есть snapshots и последующие события добавления сегментов.
    # Нельзя просто взять все mediasession из всех snapshots: один и тот же
    # сегмент тогда попадёт в итог несколько раз.
    event_logs = record.get("eventLogs") or []
    if not isinstance(event_logs, list):
        raise DownloadError("Ответ МТС Линк содержит некорректный журнал записи.")

    # Snapshot даёт первый файл каждого типа. Он может содержать несколько
    # секунд до нулевой точки записи, поэтому позже для него рассчитывается
    # trim_duration.
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

    # Реальные последующие части приходят отдельными mediasession.add.
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

    # Порядок здесь влияет только на внутренний список. В пользовательском
    # каталоге потоки дополнительно сортируются по моменту начала.
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

        # Удаляем повторные URL: API может прислать повторное событие для уже
        # известного сегмента после обновления состояния плеера.
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

    # Полная длительность записи относится к камере/спикеру, даже если
    # screen share или презентация активны только в отдельном интервале.
    try:
        total_duration = float(record.get("duration") or 0.0)
    except (TypeError, ValueError):
        total_duration = 0.0

    for stream in streams:
        if stream.key == "speaker":
            stream.duration = max(0.0, total_duration)

    return streams, max(0.0, total_duration)


def extract_presentation_streams(
    record: Dict[str, Any], total_duration: float = 0.0
) -> List[PresentationStream]:
    """Extract presentation files and the intervals when they were displayed.

    MTS Link emits presentation changes as ``presentation.update`` events.  A
    presentation therefore has no ``mediasession`` URL and cannot be handled
    by the video segment downloader.  The file URL in the event points to the
    original PDF, which is the lossless representation of this source.
    """

    event_logs = record.get("eventLogs") or []
    if not isinstance(event_logs, list):
        return []

    # Одна презентация может породить десятки presentation.update — по одному
    # на каждый переход между слайдами. Группируем их по ID файла, чтобы в
    # каталоге показывать один источник, а не десятки псевдопотоков.
    groups: Dict[str, Dict[str, Any]] = {}
    for event in event_logs:
        if not isinstance(event, dict) or event.get("module") != "presentation.update":
            continue
        data = event.get("data")
        if not isinstance(data, dict):
            continue
        file_reference = data.get("fileReference")
        if not isinstance(file_reference, dict):
            continue
        presentation_file = file_reference.get("file")
        if not isinstance(presentation_file, dict):
            continue

        # downloadUrl предпочтительнее: это ссылка на исходный PDF. Если её
        # нет, используем обычный URL файла как запасной вариант.
        source_url = presentation_file.get("downloadUrl") or presentation_file.get("url")
        if not isinstance(source_url, str) or not source_url.startswith(("http://", "https://")):
            continue

        try:
            relative_time = max(0.0, float(event.get("relativeTime", 0.0)))
        except (TypeError, ValueError):
            continue

        group_key = str(presentation_file.get("id") or source_url)
        group = groups.setdefault(
            group_key,
            {
                "name": str(presentation_file.get("name") or "presentation.pdf"),
                "source_url": source_url,
                "events": [],
                "slides": presentation_file.get("slides"),
                "displayed_slides": set(),
            },
        )
        displayed_slide = file_reference.get("slide")
        slide_url: Optional[str] = None
        slide_name: Optional[str] = None
        if isinstance(displayed_slide, dict) and displayed_slide.get("name"):
            slide_name = str(displayed_slide["name"])
            group["displayed_slides"].add(slide_name)
            candidate_slide_url = displayed_slide.get("downloadUrl") or displayed_slide.get("url")
            if isinstance(candidate_slide_url, str) and candidate_slide_url.startswith(
                ("http://", "https://")
            ):
                slide_url = candidate_slide_url
        # Сохраняем не только имя слайда, но и картинку: именно её можно
        # подать в ffmpeg без дополнительного рендеринга PDF.
        group["events"].append(
            PresentationUpdate(
                relative_time=relative_time,
                is_active=data.get("isActive") is not False,
                image_url=slide_url,
                slide_name=slide_name,
            )
        )
        if not group.get("slides") and isinstance(presentation_file.get("slides"), list):
            group["slides"] = presentation_file["slides"]

    # Если запись закончилась без финального presentation.update(false),
    # используем начало screen share или конец записи как верхнюю границу.
    # Это защищает от ошибочного растягивания последнего слайда до конца
    # двухчасовой записи.
    fallback_end = total_duration if total_duration > 0 else 0.0
    for event in event_logs:
        if not isinstance(event, dict) or event.get("module") != "mediasession.add":
            continue
        data = event.get("data")
        if _stream_key(data) != "screen-share":
            continue
        try:
            screen_start = max(0.0, float(event.get("relativeTime", 0.0)))
        except (TypeError, ValueError):
            continue
        fallback_end = screen_start if fallback_end <= 0 else min(fallback_end, screen_start)

    presentations: List[PresentationStream] = []
    for index, group in enumerate(groups.values(), start=1):
        events = sorted(group["events"], key=lambda item: item.relative_time)
        if not events:
            continue

        # Презентация может включаться и выключаться несколько раз. Храним
        # интервалы отдельно, чтобы active duration была суммой активных
        # участков, а положение в записи — общей внешней границей.
        intervals: List[Tuple[float, float]] = []
        active_start: Optional[float] = None
        for update in events:
            if not update.is_active:
                if active_start is not None and update.relative_time >= active_start:
                    intervals.append((active_start, update.relative_time))
                    active_start = None
                continue
            if active_start is None:
                active_start = update.relative_time

        if active_start is not None:
            end_time = fallback_end or events[-1].relative_time
            if end_time > active_start:
                intervals.append((active_start, end_time))

        if intervals:
            start_time = min(start for start, _ in intervals)
            end_time = max(end for _, end in intervals)
            duration = sum(end - start for start, end in intervals)
        else:
            start_time = events[0].relative_time
            end_time = events[-1].relative_time
            duration = max(0.0, end_time - start_time)

        slides = group.get("slides")
        if isinstance(slides, list):
            slide_count = sum(1 for slide in slides if isinstance(slide, dict))
        else:
            slide_count = 0

        # В некоторых записях file.slides содержит только текущий слайд, а
        # полный набор можно восстановить по update-событиям.
        slide_count = max(slide_count, len(group.get("displayed_slides") or set()))

        key = "presentation" if index == 1 else f"presentation-{index}"
        title = "Демонстрация презентации"
        if len(groups) > 1:
            title = f"{title}: {group['name']}"
        presentations.append(
            PresentationStream(
                key=key,
                title=title,
                file_name=group["name"],
                source_url=group["source_url"],
                start_time=start_time,
                duration=max(0.0, duration),
                slide_count=slide_count,
                updates=events,
            )
        )

    return presentations


def extract_media_segments(record: Dict[str, Any]) -> Tuple[List[MediaSegment], float]:
    """Вернуть сегменты камеры для старого кода, использовавшего эту функцию.

    Новая логика работает через ``extract_video_streams`` и знает о screen
    share, но оставляем этот адаптер, чтобы внешние вызовы не ломались.
    """

    streams, duration = extract_video_streams(record)
    speaker = next((stream for stream in streams if stream.key == "speaker"), None)
    if speaker:
        return speaker.segments, duration
    return streams[0].segments, duration


def _format_duration(seconds: float) -> str:
    """Отформатировать секунды как ``HH:MM:SS`` для каталога и логов."""

    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _safe_filename(name: str, fallback: str) -> str:
    """Удалить опасные для файловой системы символы из названия записи."""

    name = re.sub(r"[\\/:*?\"<>|\x00-\x1f]", "_", name).strip(" .")
    name = re.sub(r"\s+", " ", name)
    return (name or fallback)[:180] + ".mp4"


def _output_stem(
    output_dir: Path, filename: Optional[str], title: str, session_id: int
) -> Path:
    """Определить общий путь без суффикса потока и расширения.

    Один stem используется для трёх типов результатов: ``-speaker.mp4``,
    ``-presentation.pdf`` и ``-combined.mp4``. Поэтому расширение входного
    ``--filename`` здесь намеренно не сохраняется.
    """

    if filename:
        supplied = Path(filename)
        parent = supplied.parent if supplied.is_absolute() else output_dir
        stem = supplied.stem if supplied.suffix else supplied.name
        return parent / stem
    safe_title = _safe_filename(title, f"mts-link-{session_id}")[:-4]
    return output_dir / safe_title


def _probe_remote_media(url: str) -> Dict[str, Any]:
    """Получить codec, разрешение и длительность удалённого media-файла.

    ffprobe читает контейнерные метаданные через URL и не сохраняет весь файл
    локально. Если источник временно недоступен, возвращается пустой словарь:
    скачивание позже всё равно попробует этот URL и резервный HLS-вариант.
    """

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
    """Заполнить метаданные, которые выводятся в интерактивном каталоге.

    Для камеры длительность берётся из записи целиком. Для screen share она
    вычисляется как сумма длительностей его физических сегментов, потому что
    такой поток занимает только часть исходной записи.
    """

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


def print_stream_catalog(
    streams: Sequence[SelectableStream], total_duration: float
) -> None:
    """Напечатать нумерованный каталог видео, презентаций и режима C."""

    print("Доступные потоки и источники записи:", flush=True)
    for index, stream in enumerate(streams, start=1):
        print(f"\n{index}. {stream.title}", flush=True)
        if isinstance(stream, VideoStream):
            codec = stream.codec or "не определён"
            size = (
                f"{stream.width}x{stream.height}"
                if stream.width and stream.height
                else "размер неизвестен"
            )
            audio = "видео + звук" if stream.has_audio else "только видео"
            duration = (
                _format_duration(stream.duration) if stream.duration else "не определена"
            )
            interval_end = stream.end_time if stream.duration else stream.start_time
            interval = f"{_format_duration(stream.start_time)}–{_format_duration(interval_end)}"
            if stream.key == "speaker" and total_duration:
                interval = f"00:00:00–{_format_duration(total_duration)}"
            print(f"   формат: {codec}, {size}, {audio}", flush=True)
            print(
                f"   частей: {len(stream.segments)}, активная длительность: {duration}",
                flush=True,
            )
        else:
            duration = _format_duration(stream.duration) if stream.duration else "не определена"
            interval = f"{_format_duration(stream.start_time)}–{_format_duration(stream.end_time)}"
            print("   формат: PDF / слайды", flush=True)
            print(
                f"   уникальных слайдов в журнале: {stream.slide_count}, активная длительность: {duration}",
                flush=True,
            )
        print(f"   положение в записи: {interval}", flush=True)
    print("\nA. Все потоки и источники", flush=True)
    print("C. Сводное видео: материалы + спикер в правом верхнем углу", flush=True)
    print("D. Все источники и сводное видео", flush=True)


def _parse_stream_selection(value: str, stream_count: int) -> List[int]:
    """Разобрать ``A``, список номеров или диапазоны в нулевые индексы.

    Например, ``1,3`` превращается в ``[0, 2]``, а ``1-3`` — в ``[0, 1, 2]``.
    Дубли удаляются, сохраняя порядок первого появления.
    """

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
    """Старый типизированный адаптер выбора только видеопотоков."""

    return choose_streams(streams, requested)  # type: ignore[return-value]


def choose_streams(
    streams: Sequence[SelectableStream], requested: Optional[str]
) -> List[SelectableStream]:
    """Выбрать отдельные источники по CLI-значению или через prompt."""

    if requested is not None:
        try:
            indexes = _parse_stream_selection(requested, len(streams))
        except (TypeError, ValueError):
            raise DownloadError(
                "Некорректный --streams. Используйте all или номера, например 1,2."
            )
        return [streams[index] for index in indexes]

    if not sys.stdin.isatty():
        LOG.info("Интерактивный выбор недоступен, выбираю все потоки.")
        return list(streams)

    while True:
        try:
            value = input("\nВведите A для всех источников или номера через запятую: ")
            indexes = _parse_stream_selection(value, len(streams))
            return [streams[index] for index in indexes]
        except (TypeError, ValueError):
            print("Не удалось распознать выбор. Пример: A или 1,2.")


def choose_download_plan(
    streams: Sequence[SelectableStream],
    requested: Optional[str],
    composite_requested: bool = False,
    full_package_requested: bool = False,
) -> Tuple[str, List[SelectableStream]]:
    """Вернуть план скачивания: composite-режим и выбранные источники.

    ``--composite`` и буква ``C`` означают, что отдельные источники
    используются только как временные входы для одного MP4. ``D``/``full``
    сначала сохраняют все источники в выходную папку, а затем используют их
    же для сводной сборки. ``A`` и номера сохраняют прежнее поведение.
    """

    if composite_requested and full_package_requested:
        raise DownloadError("--composite и --full-package нельзя использовать вместе.")
    if (composite_requested or full_package_requested) and requested is not None:
        raise DownloadError("Режим сборки нельзя использовать вместе с --streams.")
    if composite_requested:
        return DOWNLOAD_COMPOSITE, list(streams)
    if full_package_requested:
        return DOWNLOAD_FULL_PACKAGE, list(streams)

    if requested is not None:
        normalized = requested.strip().lower()
        if normalized in {"c", "composite", "сводное", "сводное видео"}:
            return DOWNLOAD_COMPOSITE, list(streams)
        if normalized in {"d", "full", "full-package", "полный", "полный комплект"}:
            return DOWNLOAD_FULL_PACKAGE, list(streams)
        return DOWNLOAD_SEPARATE, choose_streams(streams, requested)

    if not sys.stdin.isatty():
        LOG.info("Интерактивный выбор недоступен, выбираю все потоки.")
        return DOWNLOAD_SEPARATE, list(streams)

    while True:
        try:
            value = input(
                "\nВведите C для сводного видео, D для всех источников и сводного видео, "
                "A для всех источников "
                "или номера через запятую: "
            ).strip()
            normalized = value.lower()
            if normalized in {"c", "composite", "сводное", "сводное видео"}:
                return DOWNLOAD_COMPOSITE, list(streams)
            if normalized in {"d", "full", "full-package", "полный", "полный комплект"}:
                return DOWNLOAD_FULL_PACKAGE, list(streams)
            indexes = _parse_stream_selection(value, len(streams))
            return DOWNLOAD_SEPARATE, [streams[index] for index in indexes]
        except (TypeError, ValueError):
            print("Не удалось распознать выбор. Пример: C, D, A или 1,2.")


def _ffmpeg_path() -> str:
    """Найти ffmpeg и выдать понятную ошибку, если он не установлен."""

    path = shutil.which("ffmpeg")
    if not path:
        raise DownloadError(
            "Не найден ffmpeg. Установите его отдельно, например на macOS: "
            "brew install ffmpeg."
        )
    return path


def _ffprobe_path() -> str:
    """Найти ffprobe, обычно устанавливаемый вместе с ffmpeg."""

    path = shutil.which("ffprobe")
    if not path:
        raise DownloadError(
            "Не найден ffprobe. Обычно он устанавливается вместе с ffmpeg."
        )
    return path


def _run_ffmpeg(args: Sequence[str], description: str) -> None:
    """Запустить ffmpeg с общими безопасными параметрами.

    ``-nostdin`` нужен, чтобы ffmpeg не перехватывал пользовательский prompt.
    ``-loglevel warning`` оставляет важные предупреждения, а понятное имя
    операции выводится через logging до запуска команды.
    """

    command = [_ffmpeg_path(), "-hide_banner", "-nostdin", "-loglevel", "warning"]
    command.extend(args)
    LOG.info(description)
    completed = subprocess.run(command, stdout=subprocess.DEVNULL)
    if completed.returncode != 0:
        raise DownloadError(f"ffmpeg не смог выполнить операцию: {description}")


def _probe_duration(path: Path) -> float:
    """Прочитать длительность уже скачанного локального файла через ffprobe."""

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


def _probe_stream_types(path: Path) -> set:
    """Вернуть типы потоков, реально присутствующие в локальном контейнере.

    Проверка нужна после операций ``-c copy``: если seek попал между ключевыми
    кадрами, ffmpeg иногда создаёт небольшой MP4 только с аудио или вообще без
    видеопотока. По размеру файла такую ошибку надёжно определить нельзя.
    """

    completed = subprocess.run(
        [
            _ffprobe_path(),
            "-v",
            "error",
            "-show_entries",
            "stream=codec_type",
            "-of",
            "default=nw=1:nk=1",
            str(path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return set()
    return {line.strip() for line in completed.stdout.splitlines() if line.strip()}


def _add_missing_streams(
    source: Path,
    destination: Path,
    stream: VideoStream,
) -> Path:
    """Восстановить отсутствующую дорожку в одном физическом сегменте.

    Иногда сервис сохраняет участок конференции только с аудио: например,
    камера временно не передавала кадры, но микрофон продолжал работать.
    Обратная ситуация тоже возможна — видео есть, а аудиодорожка в конкретном
    сегменте отсутствует. Такие файлы нельзя надёжно объединять с соседями
    через concat demuxer, потому что у сегментов получается разный набор
    потоков.

    Для аудио-only участка создаём чёрный видеоряд с разрешением камеры. Для
    video-only участка добавляем тишину. Перекодируется только проблемный
    короткий/единичный сегмент; нормальные исходные сегменты остаются без
    перекодирования. Возвращается ``destination`` для заменяемого локального
    файла.
    """

    stream_types = _probe_stream_types(source)
    has_video = "video" in stream_types
    has_audio = "audio" in stream_types
    if has_video and (has_audio or not stream.has_audio):
        return source
    if not has_video and not has_audio:
        raise DownloadError(f"Сегмент {source.name} не содержит ни видео, ни звука.")

    duration = _probe_duration(source)
    if duration <= 0:
        raise DownloadError(f"Не удалось определить длительность сегмента {source.name}.")

    width = stream.width or 1280
    height = stream.height or 720
    video_size = f"{width}x{height}"
    common_output = [
        "-t",
        f"{duration:.3f}",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "18",
        "-pix_fmt",
        "yuv420p",
        "-r",
        str(NORMALIZED_SOURCE_FPS),
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        "-ar",
        "48000",
        "-ac",
        "2",
        "-max_interleave_delta",
        "0",
        "-avoid_negative_ts",
        "make_zero",
        "-movflags",
        "+faststart",
        "-y",
        str(destination),
    ]

    if not has_video:
        # Видео отсутствует, но аудио есть. Ограничиваем генерацию чёрного
        # фона той же длительностью, чтобы он не стал бесконечным входом.
        LOG.warning(
            "Сегмент %s содержит только звук; добавляю чёрный видеоряд %sx%s",
            source.name,
            width,
            height,
        )
        _run_ffmpeg(
            [
                "-f",
                "lavfi",
                "-i",
                f"color=c=black:s={video_size}:r={NORMALIZED_SOURCE_FPS}:d={duration:.3f}",
                "-i",
                str(source),
                "-map",
                "0:v:0",
                "-map",
                "1:a:0",
                *common_output,
            ],
            f"Добавление отсутствующего видео к сегменту {source.name}",
        )
    else:
        # Видео есть, но для конференции нет звука. Добавляем тишину, чтобы
        # итоговый speaker-файл сохранил непрерывную аудиодорожку.
        LOG.warning(
            "Сегмент %s содержит только видео; добавляю тишину",
            source.name,
        )
        _run_ffmpeg(
            [
                "-f",
                "lavfi",
                "-i",
                f"anullsrc=r=48000:cl=stereo",
                "-i",
                str(source),
                "-map",
                "1:v:0",
                "-map",
                "0:a:0",
                *common_output,
            ],
            f"Добавление отсутствующего звука к сегменту {source.name}",
        )
    return destination


def _download_source(segment: MediaSegment, destination: Path, referer: str) -> None:
    """Скачать MP4/HLS-сегмент через ffmpeg.

    Сервер МТС Линк проверяет Referer/Origin. Сначала используется прямой MP4,
    затем HLS URL, если прямой источник не сработал. ``-c copy`` сохраняет
    исходное качество и не перекодирует физический сегмент.
    """

    headers = f"Referer: {referer}\r\nOrigin: {urlparse(referer).scheme}://{urlparse(referer).netloc}\r\n"
    candidates = [url for url in (segment.source_url, segment.hls_url) if url]
    last_error: Optional[Exception] = None
    audio_only_backup: Optional[Path] = None

    for source_url in candidates:
        try:
            destination.unlink(missing_ok=True)
            _run_ffmpeg(
                [
                    "-headers",
                    headers,
                    "-i",
                    source_url,
                    # Знак вопроса позволяет скачать и audio-only сегмент:
                    # позже _add_missing_streams добавит к нему чёрное видео.
                    "-map",
                    "0:v:0?",
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
                # Размер файла сам по себе не гарантирует, что ffmpeg записал
                # видеопоток: повреждённый/неполный MP4 может быть ненулевым.
                stream_types = _probe_stream_types(destination)
                if "video" in stream_types:
                    if audio_only_backup:
                        audio_only_backup.unlink(missing_ok=True)
                    return
                if "audio" in stream_types:
                    LOG.warning(
                        "Источник содержит только аудио; проверяю запасной URL на наличие видео"
                    )
                    # Не отбрасываем аудио-only кандидат: если HLS также не
                    # содержит видео, он всё равно нужен для сохранения звука.
                    audio_only_backup = destination.with_name(
                        destination.name + ".audio-only"
                    )
                    shutil.copy2(destination, audio_only_backup)
                    continue
                LOG.warning(
                    "Источник создал файл без видеопотока (%s), пробую следующий URL",
                    ", ".join(sorted(stream_types)) or "потоки отсутствуют",
                )
        except (DownloadError, OSError) as exc:
            last_error = exc
            LOG.warning("Источник не скачан, пробую запасной вариант: %s", source_url)

    if audio_only_backup:
        # Оба URL могут описывать один и тот же audio-only участок. В таком
        # случае возвращаем сохранённый звук, а вызывающий код добавит к нему
        # чёрное видео и продолжит сборку полного потока.
        destination.unlink(missing_ok=True)
        os.replace(audio_only_backup, destination)
        LOG.warning(
            "Видеопоток отсутствует во всех источниках; сохраняю аудио и добавлю чёрное видео"
        )
        return

    if last_error:
        raise DownloadError(f"Не удалось скачать ни один источник для сегмента: {last_error}")
    raise DownloadError("Для сегмента отсутствует URL источника.")


def _download_file(source_url: str, destination: Path, referer: str) -> None:
    """Скачать обычный бинарный ресурс, например PDF или JPG-слайд.

    Для PDF нет смысла использовать ffmpeg: сохраняем байты напрямую через
    стандартный urllib и временный путь, который вызывающий код потом
    атомарно переименует.
    """

    parsed_referer = urlparse(referer)
    request = Request(
        source_url,
        headers={
            "Referer": referer,
            "Origin": f"{parsed_referer.scheme}://{parsed_referer.netloc}",
            "User-Agent": "Mozilla/5.0",
        },
    )
    try:
        with urlopen(request, timeout=120) as response, destination.open("wb") as output:
            shutil.copyfileobj(response, output, length=1024 * 1024)
    except (OSError, URLError) as exc:
        destination.unlink(missing_ok=True)
        raise DownloadError(f"Не удалось скачать файл презентации: {exc}") from exc


def _trim_tail(source: Path, destination: Path, seconds: float) -> None:
    """Оставить хвост первого сегмента и удалить его преролл.

    ``seconds`` — это не позиция начала, а фактическая длина активной части.
    Поэтому начало вычисляется от конца исходного файла. Операция выполняется
    после скачивания, чтобы не зависеть от точности удалённого seek.

    Сначала пробуем быстрое копирование потоков. Если в коротком хвосте нет
    видеопотока из-за отсутствия ключевого кадра, повторяем операцию с
    перекодированием. Это особенно важно для первых сегментов длительностью
    несколько секунд: контейнер может быть ненулевого размера, но непригоден
    для concat demuxer.
    """
    duration = _probe_duration(source)
    if seconds <= 0 or seconds >= duration - 0.25:
        shutil.copy2(source, destination)
        return

    start = max(0.0, duration - seconds)
    trim_args = [
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
    ]
    _run_ffmpeg(trim_args, f"Отбрасывание преролла ({seconds:.1f} с)")

    source_types = _probe_stream_types(source)
    copied_types = _probe_stream_types(destination)
    required_types = {"video"}
    if "audio" in source_types:
        required_types.add("audio")
    if required_types.issubset(copied_types):
        return

    # Копирование не смогло сохранить все нужные потоки. Перекодируем только
    # короткий хвост; остальные длинные сегменты по-прежнему остаются без
    # перекодирования, поэтому обычное скачивание не становится существенно
    # медленнее.
    LOG.warning(
        "Обрезанный сегмент не содержит нужные потоки (%s), выполняю резервное перекодирование",
        ", ".join(sorted(copied_types)) or "потоки отсутствуют",
    )
    destination.unlink(missing_ok=True)
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
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "18",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-ar",
            "48000",
            "-ac",
            "2",
            "-avoid_negative_ts",
            "make_zero",
            "-y",
            str(destination),
        ],
        f"Отбрасывание преролла ({seconds:.1f} с)",
    )


def _concat_segments(segment_paths: Sequence[Path], destination: Path, duration: float) -> None:
    """Склеить локальные MP4 в один файл через concat demuxer ffmpeg.

    Все физические сегменты одного источника совместимы по кодекам. Для
    сводного режима они уже перекодированы в единый формат, поэтому тот же
    helper используется и для финального composite-файла.
    """

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
    """Открыть страницу в браузере и получить JSON записи.

    Сначала слушаем сетевой ответ самого плеера. Если ответ не пришёл
    автоматически, выполняем тот же API-запрос через Playwright context.
    ``--headed`` оставляет окно открытым для ручного входа, но cookies из
    профиля Яндекс Браузера автоматически не импортируются.
    """

    payload: Dict[str, Any] = {}
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=not headed)
        context = await browser.new_context()
        page = await context.new_page()
        page.set_default_timeout(20_000)

        async def save_record_response(response: Any) -> None:
            """Асинхронно сохранить подходящий JSON-ответ в общий payload."""

            if "/api/eventsessions/" not in response.url or "/record" not in response.url:
                return
            try:
                data = await response.json()
                if isinstance(data, dict) and data.get("eventLogs"):
                    payload.update(data)
            except Exception:
                return

        def on_response(response: Any) -> None:
            """Запланировать чтение API-ответа, не блокируя события браузера."""

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
    """Построить имя отдельного MP4 для камеры или screen share."""

    stem = _output_stem(output_dir, filename, title, session_id)
    return stem.with_name(f"{stem.name}-{stream.key}.mp4")


def presentation_output_path(
    output_dir: Path,
    filename: Optional[str],
    title: str,
    session_id: int,
    stream: PresentationStream,
) -> Path:
    """Построить имя отдельного PDF-файла презентации."""

    stem = _output_stem(output_dir, filename, title, session_id)
    return stem.with_name(f"{stem.name}-{stream.key}.pdf")


def download_stream(
    page_info: RecordingPage,
    stream: VideoStream,
    output_path: Path,
    overwrite: bool,
) -> Path:
    """Скачать и объединить все физические сегменты видеопотока.

    Алгоритм: скачать каждый URL во временную папку, обрезать преролл первого
    сегмента, склеить локальные части concat demuxer-ом и только затем
    переместить результат в пользовательскую папку.
    """

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
        # Скачиваем физические файлы по одному, чтобы не держать несколько
        # гигабайт данных в памяти и иметь резервный HLS-вариант для каждого.
        for index, segment in enumerate(segments, start=1):
            downloaded = temp_dir / f"source-{index:03d}.mp4"
            _download_source(segment, downloaded, page_info.url)

            # Для conference-потока сервер иногда отдаёт сегмент только с
            # аудио. Перед concat приводим такой сегмент к тому же набору
            # дорожек, что и соседние части: звук сохраняется, а вместо
            # отсутствующей картинки появляется чёрный видеоряд. Это важно
            # не только для отдельного MP4, но и для последующего composite,
            # где аудио спикера должно продолжаться без провала.
            prepared = _add_missing_streams(
                downloaded,
                temp_dir / f"source-{index:03d}-prepared.mp4",
                stream,
            )
            if index == 1 and segment.trim_duration is not None:
                trimmed = temp_dir / "source-001-trimmed.mp4"
                _trim_tail(prepared, trimmed, segment.trim_duration)
                local_segments.append(trimmed)
            else:
                local_segments.append(prepared)

        # Склеиваем только после обработки всех частей: так ошибка одного
        # сегмента не оставляет пользователю правдоподобный, но неполный MP4.
        combined = temp_dir / "combined.mp4"
        _concat_segments(local_segments, combined, duration)
        shutil.copy2(combined, partial_path)

    os.replace(partial_path, output_path)
    actual_duration = _probe_duration(output_path)
    LOG.info("Готово: %s (%s)", output_path, _format_duration(actual_duration))
    return output_path


def download_presentation(
    page_info: RecordingPage,
    stream: PresentationStream,
    output_path: Path,
    overwrite: bool,
) -> Path:
    """Скачать оригинальный PDF, на который ссылаются update-события."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists() and not overwrite:
        raise DownloadError(
            f"Файл уже существует: {output_path}. Добавьте --overwrite для перезаписи."
        )

    partial_path = output_path.with_name(output_path.name + ".part")
    partial_path.unlink(missing_ok=True)
    LOG.info("Скачивание презентации «%s»", stream.file_name)
    _download_file(stream.source_url, partial_path, page_info.url)
    if not partial_path.exists() or partial_path.stat().st_size == 0:
        partial_path.unlink(missing_ok=True)
        raise DownloadError("Сервис вернул пустой файл презентации.")
    os.replace(partial_path, output_path)
    LOG.info("Готово: %s (%.1f МБ)", output_path, output_path.stat().st_size / 1024 / 1024)
    return output_path


COMPOSITE_WIDTH = 1280
COMPOSITE_HEIGHT = 720
# Окно спикера уменьшено вдвое относительно исходных 320×180, чтобы не
# перекрывать содержимое презентации и screen share.
COMPOSITE_PIP_WIDTH = 160
COMPOSITE_MARGIN = 16
COMPOSITE_FPS = 25
NORMALIZED_SOURCE_FPS = 30


def _presentation_image_at(
    presentations: Sequence[PresentationStream], relative_time: float
) -> Optional[str]:
    """Вернуть слайд, активный в заданной точке общей временной шкалы."""

    for presentation in presentations:
        current_image: Optional[str] = None
        for update in presentation.updates:
            if update.relative_time > relative_time:
                break
            if not update.is_active:
                current_image = None
            elif update.image_url:
                current_image = update.image_url
        if current_image:
            return current_image
    return None


def build_composite_timeline(
    total_duration: float,
    presentations: Sequence[PresentationStream],
    screen_stream: Optional[VideoStream],
) -> List[CompositeSegment]:
    """Построить участки сводного ролика из временных меток API.

    Между соседними границами выбирается материал в приоритетном порядке:
    screen share, активный слайд, затем видео спикера. Это автоматически
    обрабатывает начало/конец демонстраций и короткие промежутки между ними.
    Соседние участки с одинаковым источником объединяются, чтобы не запускать
    лишнее перекодирование.
    """

    if total_duration <= 0:
        return []

    # Границы нужны в каждом моменте смены материала: старт/конец источника и
    # каждая presentation.update. Середина интервала затем классифицируется.
    boundaries = {0.0, total_duration}
    for presentation in presentations:
        boundaries.add(max(0.0, min(total_duration, presentation.start_time)))
        boundaries.add(max(0.0, min(total_duration, presentation.end_time)))
        for update in presentation.updates:
            if 0.0 < update.relative_time < total_duration:
                boundaries.add(update.relative_time)
    if screen_stream and screen_stream.duration > 0:
        boundaries.add(max(0.0, min(total_duration, screen_stream.start_time)))
        boundaries.add(max(0.0, min(total_duration, screen_stream.end_time)))

    ordered = sorted(boundaries)
    segments: List[CompositeSegment] = []
    for start_time, end_time in zip(ordered, ordered[1:]):
        duration = end_time - start_time
        if duration <= 0.05:
            continue
        # Проверяем середину, чтобы граница ровно в момент события относилась
        # к следующему состоянию и не создавала перекрытий.
        midpoint = start_time + duration / 2
        image_url = _presentation_image_at(presentations, midpoint)
        if (
            screen_stream
            and screen_stream.duration > 0
            and screen_stream.start_time <= midpoint < screen_stream.end_time
        ):
            kind = "screen"
            image_url = None
        elif image_url:
            kind = "presentation"
        else:
            kind = "speaker"

        if (
            segments
            and segments[-1].kind == kind
            and segments[-1].image_url == image_url
            and abs(segments[-1].start_time + segments[-1].duration - start_time) < 0.01
        ):
            segments[-1].duration += duration
        else:
            segments.append(
                CompositeSegment(
                    kind=kind,
                    start_time=start_time,
                    duration=duration,
                    image_url=image_url,
                )
            )
    return segments


def _download_composite_slide_images(
    presentations: Sequence[PresentationStream], temp_dir: Path, referer: str
) -> Dict[str, Path]:
    """Скачать уникальные JPG слайдов, необходимые для composite.

    Один и тот же слайд может появляться в журнале много раз. URL используется
    как ключ, а короткий SHA-256 — как безопасное имя локального временного
    файла.
    """

    image_paths: Dict[str, Path] = {}
    for presentation in presentations:
        for update in presentation.updates:
            if not update.image_url or update.image_url in image_paths:
                continue
            digest = hashlib.sha256(update.image_url.encode("utf-8")).hexdigest()[:16]
            destination = temp_dir / f"slide-{digest}.jpg"
            _download_file(update.image_url, destination, referer)
            if not destination.exists() or destination.stat().st_size == 0:
                raise DownloadError(f"Не удалось получить изображение слайда «{update.slide_name or digest}».")
            image_paths[update.image_url] = destination
    return image_paths


def _fit_video_filter(width: int = COMPOSITE_WIDTH, height: int = COMPOSITE_HEIGHT) -> str:
    """Сформировать ffmpeg-фильтр вписывания видео с чёрными полями."""

    return (
        f"scale={width}:{height}:force_original_aspect_ratio=decrease,"
        f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:color=black,setsar=1"
    )


def _pip_filter() -> str:
    """Сформировать фильтр масштабирования окна спикера до 160×90."""

    return (
        f"scale={COMPOSITE_PIP_WIDTH}:-2:force_original_aspect_ratio=decrease,"
        f"pad={COMPOSITE_PIP_WIDTH}:{COMPOSITE_PIP_WIDTH * 9 // 16}:(ow-iw)/2:(oh-ih)/2:color=black,setsar=1"
    )


def _video_encode_args() -> List[str]:
    """Вернуть общие параметры кодирования всех composite-сегментов."""

    return [
        "-r",
        str(COMPOSITE_FPS),
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "20",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        "-ar",
        "48000",
        "-ac",
        "2",
        "-af",
        "aresample=async=1:first_pts=0",
        # Просим MP4 muxer не откладывать слишком далеко пакеты одного из
        # потоков. Это уменьшает предупреждения о poorly interleaved packets
        # при коротких участках, вырезанных из длинной записи.
        "-max_interleave_delta",
        "0",
        "-avoid_negative_ts",
        "make_zero",
        "-y",
    ]


def _normalize_speaker_source(source: Path, destination: Path) -> None:
    """Нормализовать временные метки исходного видео спикера.

    После concat с ``-c copy`` разные MP4-сегменты могут сохранить разные
    DTS/PTS. Тогда видео декодируется с ускорением или рывками, хотя AAC-аудио
    продолжает идти нормально. Принудительный последовательный PTS для 25
    кадров/секунду и последовательный PTS аудио устраняют эту неоднозначность.
    Перекодирование выполняется только над источником, который используется
    внутри composite; отдельные скачанные MP4 остаются без лишней потери
    качества. Используется ``fps`` по исходным PTS, а не ручная формула
    ``N/(fps*TB)``: у исходных камер может быть не ровно 25 кадров/с, и такая
    формула сама способна растянуть или ускорить видео относительно аудио.
    """

    _run_ffmpeg(
        [
            "-i",
            str(source),
            "-map",
            "0:v:0",
            "-map",
            "0:a:0?",
            "-vf",
            f"setpts=PTS-STARTPTS,fps={NORMALIZED_SOURCE_FPS}",
            "-af",
            "asetpts=N/SR/TB",
            "-r",
            str(NORMALIZED_SOURCE_FPS),
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "20",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-ar",
            "48000",
            "-ac",
            "2",
            "-max_interleave_delta",
            "0",
            "-avoid_negative_ts",
            "make_zero",
            "-movflags",
            "+faststart",
            "-y",
            str(destination),
        ],
        "Нормализация временных меток видео спикера",
    )


def _render_speaker_segment(
    speaker_path: Path, segment: CompositeSegment, destination: Path
) -> None:
    """Перекодировать участок, где спикер является основным изображением."""

    _run_ffmpeg(
        [
            "-ss",
            f"{segment.start_time:.3f}",
            "-i",
            str(speaker_path),
            "-t",
            f"{segment.duration:.3f}",
            "-vf",
            _fit_video_filter(),
            "-map",
            "0:v:0",
            "-map",
            "0:a:0?",
            *_video_encode_args(),
            str(destination),
        ],
        f"Подготовка участка лекции ({_format_duration(segment.duration)})",
    )


def _render_material_segment(
    segment: CompositeSegment,
    speaker_path: Path,
    background_path: Path,
    destination: Path,
    background_offset: float = 0.0,
) -> None:
    """Создать участок материала с PiP спикера и его аудио.

    Для слайда используется зацикленный JPG. Для screen share используется
    локальный MP4 с поправкой ``background_offset``. В обоих случаях второй
    вход — общий файл спикера, из которого берутся видео и звук.
    """

    duration = f"{segment.duration:.3f}"
    if segment.kind == "presentation":
        input_args = [
            "-loop",
            "1",
            "-framerate",
            str(COMPOSITE_FPS),
            "-i",
            str(background_path),
            "-ss",
            f"{segment.start_time:.3f}",
            "-i",
            str(speaker_path),
        ]
    else:
        input_args = [
            "-ss",
            f"{background_offset:.3f}",
            "-i",
            str(background_path),
            "-ss",
            f"{segment.start_time:.3f}",
            "-i",
            str(speaker_path),
        ]
    filter_complex = (
        f"[0:v]{_fit_video_filter()}[background];"
        f"[1:v]{_pip_filter()}[speaker];"
        "[background][speaker]overlay="
        f"main_w-overlay_w-{COMPOSITE_MARGIN}:{COMPOSITE_MARGIN}:format=auto[v]"
    )
    _run_ffmpeg(
        [
            *input_args,
            "-filter_complex",
            filter_complex,
            "-map",
            "[v]",
            "-map",
            "1:a:0?",
            "-t",
            duration,
            *_video_encode_args(),
            str(destination),
        ],
        f"Наложение спикера ({_format_duration(segment.duration)})",
    )


def composite_output_path(
    output_dir: Path, filename: Optional[str], title: str, session_id: int
) -> Path:
    """Построить имя итогового файла ``*-combined.mp4``."""

    stem = _output_stem(output_dir, filename, title, session_id)
    return stem.with_name(f"{stem.name}-combined.mp4")


def download_composite(
    page_info: RecordingPage,
    speaker_stream: VideoStream,
    screen_stream: Optional[VideoStream],
    presentations: Sequence[PresentationStream],
    total_duration: float,
    output_path: Path,
    overwrite: bool,
    speaker_source: Optional[Path] = None,
    screen_source: Optional[Path] = None,
) -> Path:
    """Собрать синхронный MP4 из камеры, презентации и screen share.

    Временный pipeline такой:

    * камера скачивается (если не передан ``speaker_source``) и становится
      источником непрерывного аудио;
    * screen share скачивается (если не передан ``screen_source``) как активный
      отдельный видеопоток;
    * уникальные JPG слайдов скачиваются по presentation.update;
    * каждый участок временной шкалы перекодируется в 1280×720 H.264/AAC;
    * участки склеиваются без повторного изменения логики временной шкалы.

    Полная перекодировка нужна потому, что исходные части имеют разные
    разрешения, временные базы и типы источников (MP4 и JPG).
    """

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists() and not overwrite:
        raise DownloadError(
            f"Файл уже существует: {output_path}. Добавьте --overwrite для перезаписи."
        )

    duration = total_duration or speaker_stream.duration
    timeline = build_composite_timeline(duration, presentations, screen_stream)
    if not timeline:
        raise DownloadError("Не удалось построить временную шкалу сводного видео.")

    partial_path = output_path.with_name(output_path.name + ".part")
    partial_path.unlink(missing_ok=True)
    LOG.info(
        "Сборка сводного видео: %d участков, разрешение %dx%d",
        len(timeline),
        COMPOSITE_WIDTH,
        COMPOSITE_HEIGHT,
    )

    with tempfile.TemporaryDirectory(prefix="mts-link-composite-") as temp_name:
        temp_dir = Path(temp_name)
        if speaker_source is None:
            speaker_path = temp_dir / "speaker-source.mp4"
            download_stream(page_info, speaker_stream, speaker_path, overwrite=True)
        else:
            if not speaker_source.exists():
                raise DownloadError(f"Не найден локальный файл спикера: {speaker_source}")
            speaker_path = speaker_source

        # Нормализация обязательна и для обычного C, и для режима D. Она
        # выполняется до нарезки по временной шкале, чтобы каждый seek видел
        # равномерные PTS, а не исходные разрывы DTS между MP4-сегментами.
        normalized_speaker_path = temp_dir / "speaker-source-normalized.mp4"
        _normalize_speaker_source(speaker_path, normalized_speaker_path)

        screen_path: Optional[Path] = None
        if screen_stream:
            if screen_source is None:
                screen_path = temp_dir / "screen-source.mp4"
                download_stream(page_info, screen_stream, screen_path, overwrite=True)
            else:
                if not screen_source.exists():
                    raise DownloadError(f"Не найден локальный файл screen share: {screen_source}")
                screen_path = screen_source

        slides_dir = temp_dir / "slides"
        slides_dir.mkdir(exist_ok=True)
        image_paths = (
            _download_composite_slide_images(presentations, slides_dir, page_info.url)
            if presentations
            else {}
        )

        rendered_segments: List[Path] = []
        # Отдельные файлы нужны для concat demuxer: у каждого участка должны
        # быть одинаковые кодеки, разрешение и аудиоформат.
        for index, segment in enumerate(timeline, start=1):
            rendered = temp_dir / f"segment-{index:04d}.mp4"
            if segment.kind == "speaker":
                _render_speaker_segment(normalized_speaker_path, segment, rendered)
            elif segment.kind == "presentation":
                if not segment.image_url or segment.image_url not in image_paths:
                    raise DownloadError(
                        "Для презентации не найдено изображение слайда, необходимое для сводного видео."
                    )
                _render_material_segment(
                    segment,
                    normalized_speaker_path,
                    image_paths[segment.image_url],
                    rendered,
                )
            else:
                if not screen_path or not screen_stream:
                    raise DownloadError("Временная шкала содержит screen share без его файла.")
                _render_material_segment(
                    segment,
                    normalized_speaker_path,
                    screen_path,
                    rendered,
                    background_offset=max(0.0, segment.start_time - screen_stream.start_time),
                )
            rendered_segments.append(rendered)

        # Здесь выполняется только техническая склейка уже нормализованных
        # участков; содержимое и порядок определены timeline выше.
        combined = temp_dir / "combined.mp4"
        _concat_segments(rendered_segments, combined, duration)
        shutil.copy2(combined, partial_path)

    os.replace(partial_path, output_path)
    actual_duration = _probe_duration(output_path)
    LOG.info("Готово: %s (%s)", output_path, _format_duration(actual_duration))
    return output_path


def download_full_package(
    page_info: RecordingPage,
    streams: Sequence[VideoStream],
    presentations: Sequence[PresentationStream],
    total_duration: float,
    output_dir: Path,
    filename: Optional[str],
    title: str,
    session_id: int,
    overwrite: bool,
) -> List[Path]:
    """Скачать все отдельные источники и собрать composite из локальных файлов.

    В отличие от обычного ``C`` этот режим сначала сохраняет speaker MP4,
    screen-share MP4 и PDF презентации в выходную папку. Затем пути уже
    скачанных видео передаются в ``download_composite``, поэтому длинные
    видеопотоки не скачиваются второй раз.
    """

    output_dir.mkdir(parents=True, exist_ok=True)
    results: List[Path] = []
    local_video_paths: Dict[str, Path] = {}

    # Сначала создаём полный комплект отдельных видео. Порядок совпадает с
    # прежним режимом A: сначала камера, затем screen share.
    for stream in streams:
        output_path = stream_output_path(
            output_dir=output_dir,
            filename=filename,
            title=title,
            session_id=session_id,
            stream=stream,
        )
        result = download_stream(
            page_info=page_info,
            stream=stream,
            output_path=output_path,
            overwrite=overwrite,
        )
        local_video_paths[stream.key] = result
        results.append(result)

    # PDF остаётся отдельным исходным материалом и не заменяется слайдовым
    # MP4: это позволяет получить полный оригинальный файл презентации.
    for presentation in presentations:
        output_path = presentation_output_path(
            output_dir=output_dir,
            filename=filename,
            title=title,
            session_id=session_id,
            stream=presentation,
        )
        results.append(
            download_presentation(
                page_info=page_info,
                stream=presentation,
                output_path=output_path,
                overwrite=overwrite,
            )
        )

    speaker_stream = next((stream for stream in streams if stream.key == "speaker"), None)
    if not speaker_stream:
        raise DownloadError("Для полного комплекта не найден поток спикера.")
    composite_path = composite_output_path(
        output_dir=output_dir,
        filename=filename,
        title=title,
        session_id=session_id,
    )
    results.append(
        download_composite(
            page_info=page_info,
            speaker_stream=speaker_stream,
            screen_stream=next(
                (stream for stream in streams if stream.key == "screen-share"), None
            ),
            presentations=presentations,
            total_duration=total_duration,
            output_path=composite_path,
            overwrite=overwrite,
            speaker_source=local_video_paths["speaker"],
            screen_source=local_video_paths.get("screen-share"),
        )
    )
    return results


def build_parser() -> argparse.ArgumentParser:
    """Создать CLI-парсер с интерактивным и автоматическим режимами."""

    parser = argparse.ArgumentParser(
        description="Скачать полную запись с публичной страницы МТС Линк."
    )
    parser.add_argument("url", help="URL страницы записи МТС Линк")
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=Path("downloads"),
        help="Папка для итоговых файлов (по умолчанию: downloads)",
    )
    parser.add_argument(
        "-f",
        "--filename",
        type=str,
        help="Общее имя файлов; суффикс и расширение добавятся автоматически",
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
        help="Потоки для скачивания: all, номера, composite или full-package",
    )
    parser.add_argument(
        "--composite",
        action="store_true",
        help="Собрать презентацию, screen share и спикера в одно MP4 с PiP",
    )
    parser.add_argument(
        "--full-package",
        action="store_true",
        help="Скачать все источники и затем собрать combined.mp4 из локальных файлов",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Показывать подробные сообщения",
    )
    return parser


async def async_main(args: argparse.Namespace) -> int:
    """Выполнить полный сценарий после разбора аргументов.

    Порядок важен: сначала анализируем страницу и печатаем каталог, затем
    выбираем либо отдельные источники, либо composite. Поэтому ``--dry-run``
    никогда не начинает скачивание.
    """

    page_info = parse_recording_page(args.url)
    record = await analyze_page(page_info, headed=args.headed)

    # Из одного JSON строим две модели: видео-сегменты и презентационные
    # события. Это отражает реальную структуру API, где презентация не
    # является mediasession.
    streams, total_duration = extract_video_streams(record)
    enrich_video_streams(streams, total_duration)
    presentations = extract_presentation_streams(record, total_duration)
    # Нумерация для пользователя идёт по времени старта, поэтому презентация
    # оказывается между камерой и поздним screen share.
    available_streams: List[SelectableStream] = sorted(
        [*streams, *presentations],
        key=lambda stream: (stream.start_time, 0 if isinstance(stream, VideoStream) else 1),
    )
    print_stream_catalog(available_streams, total_duration)
    if args.dry_run:
        return 0

    composite_mode, selected_streams = choose_download_plan(
        available_streams,
        args.streams,
        composite_requested=args.composite,
        full_package_requested=args.full_package,
    )
    title = str(record.get("name") or f"mts-link-{page_info.session_id}")
    if composite_mode == DOWNLOAD_FULL_PACKAGE:
        results = download_full_package(
            page_info=page_info,
            streams=streams,
            presentations=presentations,
            total_duration=total_duration,
            output_dir=args.output_dir,
            filename=args.filename,
            title=title,
            session_id=page_info.session_id,
            overwrite=args.overwrite,
        )
        print("\nВыбрано: все источники и сводное видео", flush=True)
        for result in results:
            print(result, flush=True)
        return 0

    if composite_mode == DOWNLOAD_COMPOSITE:
        # Composite использует отдельные источники как входные данные, но
        # сохраняет только один пользовательский результат - combined.mp4.
        speaker_stream = next(
            (stream for stream in streams if stream.key == "speaker"), None
        )
        if not speaker_stream:
            raise DownloadError("Для сводного видео не найден поток спикера.")
        screen_stream = next(
            (stream for stream in streams if stream.key == "screen-share"), None
        )
        output_path = composite_output_path(
            output_dir=args.output_dir,
            filename=args.filename,
            title=title,
            session_id=page_info.session_id,
        )
        result = download_composite(
            page_info=page_info,
            speaker_stream=speaker_stream,
            screen_stream=screen_stream,
            presentations=presentations,
            total_duration=total_duration,
            output_path=output_path,
            overwrite=args.overwrite,
        )
        print("\nВыбрано: сводное видео", flush=True)
        print(result, flush=True)
        return 0

    print(
        "\nВыбрано источников:",
        ", ".join(stream.title for stream in selected_streams),
        flush=True,
    )
    # Старый режим: каждый выбранный источник получает собственный файл.
    for stream in selected_streams:
        if isinstance(stream, VideoStream):
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
        else:
            output_path = presentation_output_path(
                output_dir=args.output_dir,
                filename=args.filename,
                title=title,
                session_id=page_info.session_id,
                stream=stream,
            )
            result = download_presentation(
                page_info=page_info,
                stream=stream,
                output_path=output_path,
                overwrite=args.overwrite,
            )
        print(result, flush=True)
    return 0


def main() -> int:
    """Синхронная точка входа с единым выводом пользовательских ошибок."""

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
