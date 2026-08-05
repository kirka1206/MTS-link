#!/usr/bin/env python3
"""Скачивание и сборка записей с публичной страницы МТС Линк.

Скрипт работает в несколько независимых этапов:

1. ``analyze_page`` открывает страницу через Playwright и получает JSON-запись
   из API МТС Линк. Это позволяет использовать те же актуальные ссылки и
   временные метки, которые использует веб-плеер.
2. ``extract_video_streams`` разбирает ``mediasession`` и группирует файлы
   камеры/спикера и screen share. Один логический поток может состоять из
   нескольких последовательных MP4-сегментов.
3. Участники, подключавшиеся только с микрофоном, выделяются в отдельные
   ``AudioStream`` по ``conference.id``. Они сохраняются как M4A и могут быть
   добавлены в сводный звук с исходным временем начала.
4. ``extract_presentation_streams`` отдельно разбирает события
   ``presentation.update``. Презентация в МТС Линк не является H.264-потоком:
   API сообщает PDF и изображения активных слайдов.
5. Каталог позволяет сохранить каждый источник отдельно или выбрать режим
   ``C``/``--composite``. В этом режиме строится общая временная шкала,
   слайды превращаются в видеоряд, а видео и аудио спикера накладываются на
   демонстрируемые материалы.
6. Все временные файлы создаются в ``TemporaryDirectory`` и удаляются после
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
    # В ссылках встречаются как числовые event_id, так и значения вроде
    # ``iot_1007``. Ограничиваем поле границами сегмента URL, а не только
    # цифрами; session_id по-прежнему должен оставаться числовым.
    r"^/j/(?P<organization>[^/]+)/(?P<event_id>[^/]+)/"
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
    ``max_duration`` ограничивает длину файла с начала источника. Это нужно
    для snapshot-only записей: соседние snapshots могут перекрывать друг
    друга, поэтому каждый предыдущий файл обрезается до начала следующего.
    """

    source_url: str
    hls_url: Optional[str]
    relative_time: float
    initial: bool = False
    trim_duration: Optional[float] = None
    max_duration: Optional[float] = None


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
class AudioStream:
    """Логический аудиопоток отдельного участника записи.

    В МТС Линк вопрос слушателя может быть записан отдельной ``conference``-
    сессией без видео. Такой источник нельзя смешивать с камерой только по
    типу ``conference``: его нужно показать пользователю отдельно и добавить
    в итоговый звук с учётом ``start_time``.
    """

    key: str
    title: str
    segments: List[MediaSegment]
    duration: float
    start_time: float
    participant: Optional[str] = None
    codec: Optional[str] = None

    @property
    def end_time(self) -> float:
        """Вернуть правую границу активного интервала аудиопотока."""

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


SelectableStream = Union[VideoStream, AudioStream, PresentationStream]
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


def _media_group_key(value: Any) -> Optional[str]:
    """Вернуть стабильный ключ физической media-session.

    ``_stream_key`` намеренно возвращает общий тип ``speaker`` для всех
    конференций. Для записи этого недостаточно: камера лектора и микрофон
    слушателя имеют один тип ``conference``, но разные IDs. Группировка по ID
    позволяет восстановить их как независимые источники.
    """

    if not isinstance(value, dict):
        return None
    stream = value.get("stream") or {}
    if not isinstance(stream, dict):
        return None
    if stream.get("screensharing"):
        return "screen-share"
    conference = stream.get("conference")
    if isinstance(conference, dict):
        identifier = conference.get("id") or conference.get("publicKey")
        if identifier is not None:
            return f"conference:{identifier}"
        return "conference:unknown"
    # Оставляем запасной путь для вариантов API, где аудио может называться
    # не conference, а отдельным audio/microphone-полем.
    for media_name in ("audio", "microphone"):
        media = stream.get(media_name)
        if isinstance(media, dict):
            identifier = media.get("id") or media.get("publicKey")
            return f"audio:{identifier or media_name}"
    return None


def _collect_conference_info(record: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Собрать возможности и имя каждого conference-источника из журнала.

    Для одной конференции журнал содержит несколько ``conference.update``:
    сначала участник подключается без медиа, затем включает микрофон, а при
    завершении записи снова появляется состояние без потоков. Поэтому флаги
    ``hasVideo``/``hasAudio`` собираются операцией OR по всем состояниям, а не
    берутся только из последнего события.
    """

    event_logs = record.get("eventLogs") or []
    result: Dict[str, Dict[str, Any]] = {}

    def add_info(value: Any) -> None:
        if not isinstance(value, dict):
            return
        identifier = value.get("id")
        if identifier is None:
            return
        key = str(identifier)
        info = result.setdefault(
            key,
            {
                "has_video": False,
                "has_audio": False,
                "participant": None,
                "user_id": None,
                "participation_id": None,
            },
        )
        info["has_video"] = bool(info["has_video"] or value.get("hasVideo"))
        info["has_audio"] = bool(info["has_audio"] or value.get("hasAudio"))
        if value.get("userId") is not None:
            info["user_id"] = str(value["userId"])
        if value.get("participationId") is not None:
            info["participation_id"] = str(value["participationId"])
        user = value.get("user")
        if isinstance(user, dict):
            participant = user.get("nickname") or user.get("name")
            if participant:
                info["participant"] = str(participant)
            if user.get("id") is not None:
                info["user_id"] = str(user["id"])

    for event in event_logs:
        if not isinstance(event, dict):
            continue
        snapshot = event.get("snapshot") or {}
        snapshot_data = snapshot.get("data") if isinstance(snapshot, dict) else None
        if isinstance(snapshot_data, dict):
            for conference in snapshot_data.get("conference") or []:
                add_info(conference)

        module = event.get("module")
        if module in {"conference.add", "conference.update", "conference.delete"}:
            add_info(event.get("data"))

    return result


def _media_segment(
    value: Dict[str, Any], relative_time: float, initial: bool
) -> Optional[MediaSegment]:
    """Преобразовать одну запись ``mediasession`` в безопасную модель.

    У сегмента может быть прямой MP4 URL, HLS URL или оба варианта. Прямой
    MP4 используется первым, а HLS остаётся резервным вариантом для ffmpeg.
    """

    if not _media_group_key(value):
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


def _extract_media_streams(
    record: Dict[str, Any],
) -> Tuple[List[VideoStream], List[AudioStream], float]:
    """Извлечь видеопотоки и отдельные аудиосессии из журнала записи.

    Все ``conference``-сессии выглядят одинаково на верхнем уровне API, но
    внутри имеют собственный ``conference.id``. По этому ID и состояниям
    ``conference.update`` разделяем камеру лектора и участников, которые
    подключались только с микрофоном. Сегменты камеры разных IDs объединяются
    в один логический поток спикера, а audio-only IDs становятся отдельными
    ``AudioStream``.
    """

    event_logs = record.get("eventLogs") or []
    if not isinstance(event_logs, list):
        raise DownloadError("Ответ МТС Линк содержит некорректный журнал записи.")

    conference_info = _collect_conference_info(record)
    initial_by_group: Dict[str, MediaSegment] = {}
    additions_by_group: Dict[str, List[MediaSegment]] = {}
    try:
        total_duration = max(0.0, float(record.get("duration") or 0.0))
    except (TypeError, ValueError):
        total_duration = 0.0

    has_media_additions = any(
        isinstance(event, dict) and event.get("module") == "mediasession.add"
        for event in event_logs
    )

    if has_media_additions:
        # В обычном журнале повторные snapshots появляются на границах
        # физических cuts. Берём только первый snapshot каждого общего типа,
        # а последующие части — из mediasession.add. Это сохраняет прежнюю
        # обработку преролла и не дублирует сегмент камеры из следующего
        # snapshot.
        seen_snapshot_types: set = set()
        for event in event_logs:
            if not isinstance(event, dict):
                continue
            snapshot = event.get("snapshot") or {}
            data = snapshot.get("data") if isinstance(snapshot, dict) else None
            sessions = data.get("mediasession") if isinstance(data, dict) else None
            if not isinstance(sessions, list):
                continue
            snapshot_types: set = set()
            for session in sessions:
                broad_key = _stream_key(session) or (
                    "audio" if _media_group_key(session) else None
                )
                if not broad_key or broad_key in seen_snapshot_types:
                    snapshot_types.add(broad_key)
                    continue
                candidate = _media_segment(session, 0.0, initial=True)
                group_key = _media_group_key(session)
                if candidate and group_key and group_key not in initial_by_group:
                    initial_by_group[group_key] = candidate
                snapshot_types.add(broad_key)
            seen_snapshot_types.update(item for item in snapshot_types if item)
    else:
        # Некоторые записи не содержат ни одного mediasession.add. В них
        # каждый cut.end содержит очередной snapshot текущих медиа-сессий.
        # Snapshot является накопительным: один и тот же URL повторяется, а
        # при переподключении появляется новый URL с новым relativeTime.
        # Поэтому здесь нельзя брать только первый snapshot — это обрезает
        # запись до длительности первого физического файла.
        #
        # Для каждого логического источника сохраняем последний URL,
        # встретившийся в одной и той же временной точке. Так короткий
        # стартовый файл-преролл заменяется полноценным файлом из следующего
        # snapshot, появившимся также в t=0. Повторяющиеся URL отбрасываем.
        snapshot_candidates: Dict[str, List[MediaSegment]] = {}
        for event in event_logs:
            if not isinstance(event, dict):
                continue
            snapshot = event.get("snapshot") or {}
            data = snapshot.get("data") if isinstance(snapshot, dict) else None
            sessions = data.get("mediasession") if isinstance(data, dict) else None
            if not isinstance(sessions, list):
                continue
            try:
                relative_time = max(0.0, float(event.get("relativeTime", 0.0)))
            except (TypeError, ValueError):
                relative_time = 0.0
            for session in sessions:
                candidate = _media_segment(
                    session,
                    relative_time,
                    initial=not snapshot_candidates.get(_media_group_key(session) or ""),
                )
                group_key = _media_group_key(session)
                if not candidate or not group_key:
                    continue
                source = candidate.source_url or candidate.hls_url
                candidates = snapshot_candidates.setdefault(group_key, [])
                if any((item.source_url or item.hls_url) == source for item in candidates):
                    continue
                same_time = next(
                    (index for index, item in enumerate(candidates)
                     if abs(item.relative_time - candidate.relative_time) < 0.01),
                    None,
                )
                if same_time is None:
                    candidates.append(candidate)
                else:
                    # Поздний snapshot в той же точке времени является более
                    # полным состоянием источника, поэтому заменяет ранний.
                    candidate.initial = candidates[same_time].initial
                    candidates[same_time] = candidate

        for group_key, candidates in snapshot_candidates.items():
            candidates.sort(key=lambda item: item.relative_time)
            if not candidates:
                continue
            initial_by_group[group_key] = candidates[0]
            additions_by_group[group_key] = candidates[1:]

        # В snapshot-only формате каждый новый URL начинает новый физический
        # интервал. Ограничиваем предыдущий файл моментом следующего URL, а
        # последний — концом записи. Это убирает перекрытия между файлами и
        # не позволяет отдельный источник сделать длиннее самой записи.
        for candidates in snapshot_candidates.values():
            candidates.sort(key=lambda item: item.relative_time)
            for index, segment in enumerate(candidates):
                if index + 1 < len(candidates):
                    next_time = candidates[index + 1].relative_time
                else:
                    next_time = total_duration
                if next_time > segment.relative_time:
                    segment.max_duration = next_time - segment.relative_time

    # Реальные новые части приходят отдельными mediasession.add и сохраняют
    # relativeTime, необходимый как для склейки, так и для аудиомикширования.
    for event in event_logs:
        if not isinstance(event, dict) or event.get("module") != "mediasession.add":
            continue
        data = event.get("data")
        if not isinstance(data, dict):
            continue
        try:
            relative_time = float(event.get("relativeTime", 0.0))
        except (TypeError, ValueError):
            continue
        candidate = _media_segment(data, relative_time, initial=False)
        group_key = _media_group_key(data)
        if candidate and group_key:
            additions_by_group.setdefault(group_key, []).append(candidate)

    speaker_segments: List[MediaSegment] = []
    screen_stream: Optional[VideoStream] = None
    audio_streams: List[AudioStream] = []
    participant_video_streams: List[VideoStream] = []
    all_groups = set(initial_by_group) | set(additions_by_group)

    # Первый video-capable conference считаем камерой лектора. Если лектор
    # переподключался, его последующие conference IDs объединяются по
    # user_id; видео другого участника остаётся отдельным источником.
    video_group_keys = [
        group_key
        for group_key in all_groups
        if group_key.startswith("conference:")
        and conference_info.get(group_key.split(":", 1)[1], {}).get("has_video")
    ]
    primary_video_group: Optional[str] = None
    if video_group_keys:
        primary_video_group = min(
            video_group_keys,
            key=lambda group_key: (
                (initial_by_group.get(group_key) or (additions_by_group.get(group_key) or [None])[0]).relative_time
                if (initial_by_group.get(group_key) or additions_by_group.get(group_key))
                else float("inf")
            ),
        )
    primary_info = (
        conference_info.get(primary_video_group.split(":", 1)[1], {})
        if primary_video_group
        else {}
    )
    primary_user_id = primary_info.get("user_id")

    for group_key in all_groups:
        initial = initial_by_group.get(group_key)
        additions = sorted(
            additions_by_group.get(group_key, []),
            key=lambda item: item.relative_time,
        )
        segments: List[MediaSegment] = []
        seen_sources: set = set()
        if initial:
            initial_source = initial.source_url or initial.hls_url
            if initial_source:
                segments.append(initial)
                seen_sources.add(initial_source)
        for candidate in additions:
            candidate_source = candidate.source_url or candidate.hls_url
            if not candidate_source or candidate_source in seen_sources:
                continue
            segments.append(candidate)
            seen_sources.add(candidate_source)
        if not segments:
            continue

        if has_media_additions and initial and additions:
            # Только первый snapshot-файл может содержать преролл до нулевой
            # точки записи. Для audio-only initial это работает тем же образом.
            segments[0].trim_duration = max(0.0, additions[0].relative_time)

        if group_key == "screen-share":
            screen_stream = VideoStream(
                key="screen-share",
                title="Расшаренный экран",
                segments=segments,
                duration=0.0,
                start_time=segments[0].relative_time,
                has_audio=False,
            )
            continue

        if not group_key.startswith("conference:"):
            audio_streams.append(
                AudioStream(
                    key=f"audio-{len(audio_streams) + 1}",
                    title=f"Аудиопоток {len(audio_streams) + 1}",
                    segments=segments,
                    duration=0.0,
                    start_time=segments[0].relative_time,
                )
            )
            continue

        conference_id = group_key.split(":", 1)[1]
        info = conference_info.get(conference_id)
        # В старых/неполных журналах conference.update может отсутствовать.
        # Сохраняем обратную совместимость: неизвестные conference считаем
        # камерой, как делала прежняя версия скрипта.
        is_audio_only = bool(info and info.get("has_audio") and not info.get("has_video"))
        if is_audio_only:
            participant = info.get("participant") if info else None
            title = f"Аудио участника: {participant}" if participant else "Аудио участника"
            audio_streams.append(
                AudioStream(
                    key=f"audio-{conference_id}",
                    title=title,
                    segments=segments,
                    duration=0.0,
                    start_time=segments[0].relative_time,
                    participant=participant,
                )
            )
        else:
            if (
                info
                and info.get("has_video")
                and primary_user_id
                and info.get("user_id")
                and info.get("user_id") != primary_user_id
            ):
                participant = info.get("participant")
                title = f"Видео участника: {participant}" if participant else "Видео участника"
                participant_video_streams.append(
                    VideoStream(
                        key=f"participant-video-{conference_id}",
                        title=title,
                        segments=segments,
                        duration=0.0,
                        start_time=segments[0].relative_time,
                        has_audio=bool(info.get("has_audio")),
                    )
                )
            else:
                speaker_segments.extend(segments)

    speaker_segments.sort(key=lambda item: item.relative_time)
    streams: List[VideoStream] = []
    if speaker_segments:
        streams.append(
            VideoStream(
                key="speaker",
                title="Спикер / камера",
                segments=speaker_segments,
                duration=total_duration,
                start_time=0.0,
                has_audio=True,
            )
        )
    streams.extend(participant_video_streams)
    if screen_stream:
        streams.append(screen_stream)

    if not streams and not audio_streams:
        raise DownloadError(
            "На странице не найден ни один медиапоток. "
            "Возможно, запись удалена или доступ к ней ограничен."
        )
    return streams, audio_streams, total_duration


def extract_video_streams(record: Dict[str, Any]) -> Tuple[List[VideoStream], float]:
    """Вернуть видеопотоки, сохраняя прежнюю сигнатуру публичной функции."""

    # Основная реализация теперь возвращает также audio-only источники. Этот
    # адаптер оставляет прежнюю сигнатуру функции для внешних пользователей.
    streams, _, total_duration = _extract_media_streams(record)
    return streams, total_duration


def extract_audio_streams(record: Dict[str, Any]) -> Tuple[List[AudioStream], float]:
    """Вернуть отдельные audio-only потоки и длительность записи."""

    _, audio_streams, total_duration = _extract_media_streams(record)
    return audio_streams, total_duration


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

    Один stem используется для разных типов результатов: ``-speaker.mp4``,
    ``-audio-<id>.m4a``, ``-presentation.pdf`` и ``-combined.mp4``. Поэтому
    расширение входного ``--filename`` здесь намеренно не сохраняется.
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


def _probe_remote_audio(url: str) -> Dict[str, Any]:
    """Получить codec и длительность удалённого аудиофайла."""

    completed = subprocess.run(
        [
            _ffprobe_path(),
            "-v",
            "error",
            "-select_streams",
            "a:0",
            "-show_entries",
            "stream=codec_name,sample_rate,channels:format=duration",
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
                effective_duration = float(duration)
                if segment.max_duration is not None:
                    effective_duration = min(effective_duration, segment.max_duration)
                segment_duration += effective_duration
        if stream.segments and stream.segments[0].trim_duration is not None:
            segment_duration = min(segment_duration, stream.segments[0].trim_duration)
        stream.duration = max(0.0, segment_duration)


def enrich_audio_streams(audio_streams: Sequence[AudioStream]) -> None:
    """Заполнить длительность и codec отдельных аудиопотоков."""

    for stream in audio_streams:
        active_duration = 0.0
        first_url = stream.segments[0].source_url or stream.segments[0].hls_url
        if first_url:
            metadata = _probe_remote_audio(first_url)
            stream.codec = metadata.get("codec_name")
        for segment in stream.segments:
            source_url = segment.source_url or segment.hls_url
            if not source_url:
                continue
            metadata = _probe_remote_audio(source_url)
            duration = metadata.get("duration")
            if isinstance(duration, (int, float)):
                effective_duration = float(duration)
                if segment.max_duration is not None:
                    effective_duration = min(effective_duration, segment.max_duration)
                active_duration += effective_duration
        if stream.segments and stream.segments[0].trim_duration is not None:
            active_duration = min(active_duration, stream.segments[0].trim_duration)
        stream.duration = max(0.0, active_duration)


def print_stream_catalog(
    streams: Sequence[SelectableStream], total_duration: float
) -> None:
    """Напечатать нумерованный каталог видео, аудио, презентаций и режимов."""

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
        elif isinstance(stream, AudioStream):
            codec = stream.codec or "не определён"
            duration = _format_duration(stream.duration) if stream.duration else "не определена"
            interval = f"{_format_duration(stream.start_time)}–{_format_duration(stream.end_time)}"
            print(f"   формат: {codec}, только звук", flush=True)
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


def _create_video_gap(
    destination: Path,
    duration: float,
    stream: VideoStream,
) -> None:
    """Создать чёрный заполнитель для паузы между видеосегментами.

    После отделения audio-only участника у камеры физически появляется
    временной разрыв. Без заполнителя concat сдвинул бы следующий кадр
    вперёд на несколько минут. Чёрный фон сохраняет положение камеры в
    общей временной шкале, а тишина не подменяет отдельный аудиопоток.
    """

    width = stream.width or 1280
    height = stream.height or 720
    args = [
        "-f",
        "lavfi",
        "-i",
        f"color=c=black:s={width}x{height}:r={NORMALIZED_SOURCE_FPS}:d={duration:.3f}",
    ]
    if stream.has_audio:
        args.extend(
            [
                "-f",
                "lavfi",
                "-i",
                "anullsrc=r=48000:cl=stereo",
                "-map",
                "0:v:0",
                "-map",
                "1:a:0",
            ]
        )
    else:
        args.extend(["-map", "0:v:0"])
    args.extend(
        [
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
        ]
    )
    if stream.has_audio:
        args.extend(["-c:a", "aac", "-b:a", "192k", "-ar", "48000", "-ac", "2"])
    args.extend(["-avoid_negative_ts", "make_zero", "-y", str(destination)])
    _run_ffmpeg(args, f"Заполнение паузы камеры ({_format_duration(duration)})")


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


def _download_audio_source(segment: MediaSegment, destination: Path, referer: str) -> None:
    """Скачать только аудиодорожку физического conference-сегмента."""

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
                    "0:a:0",
                    "-vn",
                    "-c:a",
                    "copy",
                    "-movflags",
                    "+faststart",
                    "-y",
                    str(destination),
                ],
                f"Скачивание аудиоисточника {source_url.rsplit('/', 1)[-1]}",
            )
            if destination.exists() and destination.stat().st_size > 0:
                if "audio" in _probe_stream_types(destination):
                    return
        except (DownloadError, OSError) as exc:
            last_error = exc
            LOG.warning("Аудиоисточник не скачан, пробую запасной вариант: %s", source_url)

    if last_error:
        raise DownloadError(f"Не удалось скачать аудиоисточник: {last_error}")
    raise DownloadError("Для аудиопотока отсутствует URL источника.")


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


def _trim_head(source: Path, destination: Path, seconds: float) -> None:
    """Оставить первые ``seconds`` секунд MP4-сегмента.

    В отличие от ``_trim_tail`` эта функция удаляет хвост. Она применяется
    только к snapshot-only источникам, где следующий URL уже обозначает
    точку замены предыдущего источника. Сначала используется копирование
    потоков, а при проблеме с ключевым кадром — короткое перекодирование с
    сохранением всех доступных дорожек.
    """

    duration = _probe_duration(source)
    if seconds <= 0:
        raise DownloadError("Нельзя создать сегмент нулевой длительности.")
    if seconds >= duration - 0.25:
        shutil.copy2(source, destination)
        return

    trim_args = [
        "-i",
        str(source),
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
    _run_ffmpeg(trim_args, f"Ограничение перекрывающегося сегмента ({seconds:.1f} с)")

    source_types = _probe_stream_types(source)
    copied_types = _probe_stream_types(destination)
    required_types = {"video"}
    if "audio" in source_types:
        required_types.add("audio")
    if required_types.issubset(copied_types):
        return

    LOG.warning(
        "Обрезанный snapshot не содержит нужные потоки (%s), выполняю резервное перекодирование",
        ", ".join(sorted(copied_types)) or "потоки отсутствуют",
    )
    destination.unlink(missing_ok=True)
    _run_ffmpeg(
        [
            "-i",
            str(source),
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
        f"Ограничение перекрывающегося сегмента ({seconds:.1f} с)",
    )


def _trim_audio_head(source: Path, destination: Path, seconds: float) -> None:
    """Оставить первые ``seconds`` секунд аудиосегмента snapshot-only записи."""

    duration = _probe_duration(source)
    if seconds <= 0:
        raise DownloadError("Нельзя создать аудиосегмент нулевой длительности.")
    if seconds >= duration - 0.25:
        shutil.copy2(source, destination)
        return

    _run_ffmpeg(
        [
            "-i",
            str(source),
            "-t",
            f"{seconds:.3f}",
            "-map",
            "0:a:0",
            "-vn",
            "-c:a",
            "copy",
            "-avoid_negative_ts",
            "make_zero",
            "-y",
            str(destination),
        ],
        f"Ограничение перекрывающегося аудиосегмента ({seconds:.1f} с)",
    )
    if "audio" in _probe_stream_types(destination):
        return

    destination.unlink(missing_ok=True)
    _run_ffmpeg(
        [
            "-i",
            str(source),
            "-t",
            f"{seconds:.3f}",
            "-map",
            "0:a:0",
            "-vn",
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
        f"Ограничение перекрывающегося аудиосегмента ({seconds:.1f} с)",
    )


def _trim_audio_tail(source: Path, destination: Path, seconds: float) -> None:
    """Оставить активный хвост audio-only сегмента без требования видео."""

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
            "0:a:0",
            "-vn",
            "-c:a",
            "copy",
            "-avoid_negative_ts",
            "make_zero",
            "-y",
            str(destination),
        ],
        f"Обрезание преролла аудиопотока ({seconds:.1f} с)",
    )
    if "audio" in _probe_stream_types(destination):
        return

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
            "0:a:0",
            "-vn",
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
        f"Обрезание преролла аудиопотока ({seconds:.1f} с)",
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


def _concat_audio_segments(
    segment_paths: Sequence[Path], destination: Path, duration: float
) -> None:
    """Склеить локальные аудиочасти в один M4A без видеопотока."""

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
            "0:a:0",
            "-vn",
            "-c:a",
            "copy",
            "-movflags",
            "+faststart",
        ]
        if duration > 0:
            args.extend(["-t", f"{duration:.3f}"])
        args.extend(["-y", str(destination)])
        _run_ffmpeg(args, "Объединение аудиосегментов")
    finally:
        list_path.unlink(missing_ok=True)


def _create_silence_audio(destination: Path, duration: float) -> None:
    """Создать AAC-фрагмент тишины для паузы между аудиосегментами."""

    _run_ffmpeg(
        [
            "-f",
            "lavfi",
            "-i",
            "anullsrc=r=48000:cl=stereo",
            "-t",
            f"{duration:.3f}",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-ar",
            "48000",
            "-ac",
            "2",
            "-movflags",
            "+faststart",
            "-y",
            str(destination),
        ],
        f"Заполнение паузы аудиопотока ({_format_duration(duration)})",
    )


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


def audio_output_path(
    output_dir: Path,
    filename: Optional[str],
    title: str,
    session_id: int,
    stream: AudioStream,
) -> Path:
    """Построить имя отдельного M4A-файла для audio-only участника."""

    stem = _output_stem(output_dir, filename, title, session_id)
    return stem.with_name(f"{stem.name}-{stream.key}.m4a")


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
        timeline_position = 0.0
        # Скачиваем физические файлы по одному, чтобы не держать несколько
        # гигабайт данных в памяти и иметь резервный HLS-вариант для каждого.
        for index, segment in enumerate(segments, start=1):
            # После выделения audio-only conference из speaker-потока между
            # соседними видеофайлами может быть настоящая пауза. Вставляем
            # чёрный участок, иначе последующая камера и screen share
            # окажутся раньше своего места в записи.
            if (
                stream.key == "speaker"
                and index > 1
                and segment.relative_time > timeline_position + 0.5
            ):
                gap_duration = segment.relative_time - timeline_position
                gap = temp_dir / f"source-{index:03d}-gap.mp4"
                _create_video_gap(gap, gap_duration, stream)
                local_segments.append(gap)
                timeline_position += gap_duration

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
            if segment.max_duration is not None:
                # В snapshot-only журнале relativeTime обозначает начало
                # нового физического файла. Ограничиваем его с начала, чтобы
                # предыдущий snapshot не дублировал уже заменённый материал.
                if segment.max_duration <= 0.0:
                    LOG.warning(
                        "Пропускаю snapshot с нулевым интервалом: %s",
                        segment.source_url or segment.hls_url,
                    )
                    continue
                limited = temp_dir / f"source-{index:03d}-limited.mp4"
                _trim_head(prepared, limited, segment.max_duration)
                prepared = limited
            if index == 1 and segment.trim_duration is not None:
                trimmed = temp_dir / "source-001-trimmed.mp4"
                _trim_tail(prepared, trimmed, segment.trim_duration)
                local_segments.append(trimmed)
                timeline_position += _probe_duration(trimmed)
            else:
                local_segments.append(prepared)
                timeline_position += _probe_duration(prepared)

        # Склеиваем только после обработки всех частей: так ошибка одного
        # сегмента не оставляет пользователю правдоподобный, но неполный MP4.
        combined = temp_dir / "combined.mp4"
        _concat_segments(local_segments, combined, duration)
        shutil.copy2(combined, partial_path)

    os.replace(partial_path, output_path)
    actual_duration = _probe_duration(output_path)
    LOG.info("Готово: %s (%s)", output_path, _format_duration(actual_duration))
    return output_path


def download_audio_stream(
    page_info: RecordingPage,
    stream: AudioStream,
    output_path: Path,
    overwrite: bool,
) -> Path:
    """Скачать и склеить все части отдельного аудиопотока в M4A."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists() and not overwrite:
        raise DownloadError(
            f"Файл уже существует: {output_path}. Добавьте --overwrite для перезаписи."
        )

    LOG.info("Аудиопоток «%s»: сегментов %d", stream.title, len(stream.segments))
    if stream.duration:
        LOG.info("Активная длительность аудиопотока: %s", _format_duration(stream.duration))

    partial_path = output_path.with_name(output_path.name + ".part")
    partial_path.unlink(missing_ok=True)
    with tempfile.TemporaryDirectory(prefix="mts-link-audio-") as temp_name:
        temp_dir = Path(temp_name)
        local_segments: List[Path] = []
        timeline_position = 0.0
        for index, segment in enumerate(stream.segments, start=1):
            expected_position = max(0.0, segment.relative_time - stream.start_time)
            if index > 1 and expected_position > timeline_position + 0.5:
                gap_duration = expected_position - timeline_position
                gap = temp_dir / f"source-{index:03d}-gap.m4a"
                _create_silence_audio(gap, gap_duration)
                local_segments.append(gap)
                timeline_position += gap_duration

            downloaded = temp_dir / f"source-{index:03d}.m4a"
            _download_audio_source(segment, downloaded, page_info.url)
            if segment.max_duration is not None:
                # Та же логика нужна отдельным аудиопотокам участников: при
                # переподключении новый snapshot может перекрывать старый.
                if segment.max_duration <= 0.0:
                    LOG.warning(
                        "Пропускаю аудиоснимок с нулевым интервалом: %s",
                        segment.source_url or segment.hls_url,
                    )
                    continue
                limited = temp_dir / f"source-{index:03d}-limited.m4a"
                _trim_audio_head(downloaded, limited, segment.max_duration)
                downloaded = limited
            if index == 1 and segment.trim_duration is not None:
                trimmed = temp_dir / "source-001-trimmed.m4a"
                _trim_audio_tail(downloaded, trimmed, segment.trim_duration)
                local_segments.append(trimmed)
                timeline_position += _probe_duration(trimmed)
            else:
                local_segments.append(downloaded)
                timeline_position += _probe_duration(downloaded)

        combined = temp_dir / "combined.m4a"
        _concat_audio_segments(local_segments, combined, max(stream.duration, timeline_position))
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


def _mix_audio_streams(
    speaker_path: Path,
    audio_streams: Sequence[AudioStream],
    audio_sources: Dict[str, Path],
    destination: Path,
    duration: float,
) -> None:
    """Смешать основной звук камеры с аудио участников по временной шкале."""

    if not audio_streams:
        raise DownloadError("Для микширования не передан ни один аудиопоток.")

    input_args: List[str] = ["-i", str(speaker_path)]
    filters = ["[0:a:0]aresample=48000,asetpts=PTS-STARTPTS[main]"]
    mix_inputs = ["[main]"]
    for index, stream in enumerate(audio_streams, start=1):
        source = audio_sources.get(stream.key)
        if not source or not source.exists():
            raise DownloadError(f"Не найден локальный файл аудиопотока: {stream.title}")
        input_args.extend(["-i", str(source)])
        label = f"participant{index}"
        delay_ms = max(0, int(round(stream.start_time * 1000)))
        filters.append(
            f"[{index}:a:0]aresample=48000,asetpts=PTS-STARTPTS,"
            f"adelay={delay_ms}:all=1[{label}]"
        )
        mix_inputs.append(f"[{label}]")
    filters.append(
        "".join(mix_inputs)
        + f"amix=inputs={len(mix_inputs)}:duration=first:dropout_transition=0:"
        "normalize=1,aresample=async=1:first_pts=0[mixed]"
    )
    _run_ffmpeg(
        [
            *input_args,
            "-filter_complex",
            ";".join(filters),
            "-map",
            "[mixed]",
            "-vn",
            "-t",
            f"{duration:.3f}",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-ar",
            "48000",
            "-ac",
            "2",
            "-movflags",
            "+faststart",
            "-y",
            str(destination),
        ],
        "Микширование аудио спикера и участников",
    )


def _render_speaker_segment(
    speaker_path: Path,
    segment: CompositeSegment,
    destination: Path,
    audio_path: Optional[Path] = None,
) -> None:
    """Перекодировать участок, где спикер является основным изображением."""

    input_args = [
        "-ss",
        f"{segment.start_time:.3f}",
        "-i",
        str(speaker_path),
    ]
    audio_index = 0
    if audio_path and audio_path != speaker_path:
        # mixed-аудио содержит всю запись целиком. Для текущего фрагмента
        # нужно перемотать его к той же позиции, что и видео спикера; иначе
        # каждый участок composite начинал бы звук снова с 00:00:00.
        input_args.extend(["-ss", f"{segment.start_time:.3f}", "-i", str(audio_path)])
        audio_index = 1
    _run_ffmpeg(
        [
            *input_args,
            "-t",
            f"{segment.duration:.3f}",
            "-vf",
            _fit_video_filter(),
            "-map",
            "0:v:0",
            "-map",
            f"{audio_index}:a:0?",
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
    audio_path: Optional[Path] = None,
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
    audio_index = 1
    if audio_path and audio_path != speaker_path:
        # Аналогично ветке speaker: дополнительный звук является глобальной
        # дорожкой записи и должен читаться с позиции текущего участка.
        input_args.extend(["-ss", f"{segment.start_time:.3f}", "-i", str(audio_path)])
        audio_index = 2
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
            f"{audio_index}:a:0?",
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
    audio_streams: Sequence[AudioStream] = (),
    audio_sources: Optional[Dict[str, Path]] = None,
) -> Path:
    """Собрать синхронный MP4 из камеры, презентации и screen share.

    Временный pipeline такой:

    * камера скачивается (если не передан ``speaker_source``) и становится
      источником непрерывного аудио;
    * отдельные аудиосессии участников скачиваются и микшируются с камерой
      по их исходным relativeTime;
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

        local_audio_sources: Dict[str, Path] = {}
        supplied_audio_sources = audio_sources or {}
        for audio_stream in audio_streams:
            supplied = supplied_audio_sources.get(audio_stream.key)
            if supplied is not None:
                if not supplied.exists():
                    raise DownloadError(f"Не найден локальный файл аудиопотока: {supplied}")
                local_audio_sources[audio_stream.key] = supplied
            else:
                local_audio = temp_dir / f"{audio_stream.key}.m4a"
                download_audio_stream(
                    page_info=page_info,
                    stream=audio_stream,
                    output_path=local_audio,
                    overwrite=True,
                )
                local_audio_sources[audio_stream.key] = local_audio

        mixed_audio_path = normalized_speaker_path
        if audio_streams:
            mixed_audio_path = temp_dir / "speaker-and-participants.m4a"
            _mix_audio_streams(
                speaker_path=normalized_speaker_path,
                audio_streams=audio_streams,
                audio_sources=local_audio_sources,
                destination=mixed_audio_path,
                duration=duration,
            )

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
                _render_speaker_segment(
                    normalized_speaker_path,
                    segment,
                    rendered,
                    audio_path=mixed_audio_path,
                )
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
                    audio_path=mixed_audio_path,
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
                    audio_path=mixed_audio_path,
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
    audio_streams: Sequence[AudioStream],
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
    local_audio_paths: Dict[str, Path] = {}

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

    # Отдельные микрофоны слушателей сохраняются в M4A. Их временные позиции
    # затем используются при сборке combined.mp4, а сами файлы остаются в
    # полном комплекте для независимого прослушивания.
    for audio_stream in audio_streams:
        output_path = audio_output_path(
            output_dir=output_dir,
            filename=filename,
            title=title,
            session_id=session_id,
            stream=audio_stream,
        )
        result = download_audio_stream(
            page_info=page_info,
            stream=audio_stream,
            output_path=output_path,
            overwrite=overwrite,
        )
        local_audio_paths[audio_stream.key] = result
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
            audio_streams=audio_streams,
            audio_sources=local_audio_paths,
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
    streams, audio_streams, total_duration = _extract_media_streams(record)
    enrich_video_streams(streams, total_duration)
    enrich_audio_streams(audio_streams)
    presentations = extract_presentation_streams(record, total_duration)
    # Нумерация для пользователя идёт по времени старта, поэтому презентация
    # оказывается между камерой и поздним screen share.
    available_streams: List[SelectableStream] = sorted(
        [*streams, *audio_streams, *presentations],
        key=lambda stream: (
            stream.start_time,
            0 if isinstance(stream, VideoStream) else 1 if isinstance(stream, AudioStream) else 2,
        ),
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
            audio_streams=audio_streams,
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
            audio_streams=audio_streams,
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
        elif isinstance(stream, AudioStream):
            output_path = audio_output_path(
                output_dir=args.output_dir,
                filename=args.filename,
                title=title,
                session_id=page_info.session_id,
                stream=stream,
            )
            result = download_audio_stream(
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
