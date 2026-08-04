# Скачивание записей МТС Линк

Утилита принимает URL страницы записи МТС Линк, проверяет страницу в браузере, показывает все найденные видеопотоки и предлагает выбрать нужные. Каждый выбранный поток сохраняется отдельным MP4-файлом.

## Установка

Нужны Python 3.9+ и `ffmpeg`/`ffprobe` в `PATH`.

На macOS:

```bash
brew install ffmpeg
python3 -m venv venv
source venv/bin/activate
python -m pip install -r requirements.txt
python -m playwright install chromium
```

## Запуск

```bash
source venv/bin/activate
python download_mts_link.py \
  "https://my.mts-link.ru/j/Deckhouse/19443161368/record-new/18659320070" \
  --output-dir downloads
```

Перед большим скачиванием можно проверить, что страница разобрана корректно:

```bash
python download_mts_link.py "URL_ЗАПИСИ" --dry-run
```

При обычном запуске утилита выведет, например:

```text
1. Спикер / камера       1280x720, видео + звук
2. Расшаренный экран     1920x1056, только видео
A. Все потоки
```

После этого можно ввести `A` или номера через запятую, например `1,2`. По умолчанию файлы будут созданы в `downloads` с суффиксами `-speaker.mp4` и `-screen-share.mp4`.

Для неинтерактивного запуска:

```bash
python download_mts_link.py "URL_ЗАПИСИ" --streams all
python download_mts_link.py "URL_ЗАПИСИ" --streams 2
```

Для своего общего имени:

```bash
python download_mts_link.py "URL_ЗАПИСИ" -o downloads -f lecture
```

В этом случае будут созданы `lecture-speaker.mp4` и `lecture-screen-share.mp4`. Для screen-share сохраняется активная часть потока; каталог показывает её положение внутри исходной записи.

Если ссылка требует входа, запустите с `--headed`, выполните вход в открывшемся окне и нажмите Enter в терминале:

```bash
python download_mts_link.py "URL_ЗАПИСИ" --headed
```

Для перезаписи существующего файла добавьте `--overwrite`. Параметр `--verbose` включает подробный журнал.

## Ограничения

- Утилита рассчитана на публичные ссылки МТС Линк формата `/j/.../record-new/...`.
- Нужен установленный `ffmpeg`; он не устанавливается через `pip`.
- Браузер используется для проверки доступности страницы и получения актуального описания записи. Утилита не импортирует cookies из профиля Яндекс Браузера.
