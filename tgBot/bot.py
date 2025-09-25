import re
import json
import base64
import asyncio
import logging
import tempfile
import shutil
import time
import random
from uuid import uuid4
from contextlib import suppress
from pathlib import Path
from urllib.parse import urlparse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import aiogram
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import (
    Message, FSInputFile, BotCommand,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.exceptions import TelegramBadRequest

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from yt_dlp import YoutubeDL
from dotenv import load_dotenv
import os

# ==================== Конфиг ====================
load_dotenv()

from pathlib import Path
BASE = Path(__file__).resolve().parent
QUEUE = BASE / "queue"


# и перед отправкой:

# --- CHANNEL_ID с поддержкой -100... и @username
_CH = os.getenv("CHANNEL_ID", "").strip()
def parse_channel_id(_ch: str):
    if not _ch:
        return None
    if _ch.startswith("@"):
        return _ch
    try:
        return int(_ch)  # поддерживает и отрицательные id
    except Exception:
        return None
CHANNEL_ID = parse_channel_id(_CH)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("Не задан BOT_TOKEN в .env")
if CHANNEL_ID is None:
    print("ВНИМАНИЕ: CHANNEL_ID не задан — автопостинг в канал не будет работать (ручные «🚀» можно слать в текущий чат).")

# Таймзона и окна
TZ = ZoneInfo(os.getenv("TZ", "Europe/Helsinki"))
POST_WINDOW_START = int(os.getenv("POST_WINDOW_START", "8"))
POST_WINDOW_END   = int(os.getenv("POST_WINDOW_END", "23"))  # не включительно
RANDOM_DAYS_MAX   = int(os.getenv("RANDOM_DAYS_MAX", "2"))
DELAY_LOCAL_PREP = os.getenv("DELAY_LOCAL_PREP", "0").lower() in ("1", "true", "yes", "on")


# Просроченные задачи: как часто проверять и сколько отправлять за один тик
OVERDUE_SCAN_MINUTES = int(os.getenv("OVERDUE_SCAN_MINUTES", "5"))   # каждые 5 минут
OVERDUE_PER_TICK     = int(os.getenv("OVERDUE_PER_TICK", "1"))

# Хвост видео
TRIM_TAIL_SEC = float(os.getenv("TRIM_TAIL_SEC", "0") or "0")

# Очередь
BASE_DIR  = Path(__file__).resolve().parent
QUEUE_DIR = BASE_DIR / "queue"
TASKS_DIR = QUEUE_DIR / "tasks"
MEDIA_DIR = QUEUE_DIR / "media"
for d in (QUEUE_DIR, TASKS_DIR, MEDIA_DIR):
    d.mkdir(parents=True, exist_ok=True)

# yt-dlp / cookies
COOKIES_BROWSER = os.getenv("COOKIES_BROWSER", "").strip() or None
COOKIES_PROFILE = os.getenv("COOKIES_PROFILE", "").strip() or None

# История
HISTORY_ENABLED = os.getenv("HISTORY_ENABLED", "1").lower() in ("1","true","yes","on")
HISTORY_FILE = (QUEUE_DIR / "history.jsonl")
HIST_HASHES: set[str] = set()
HIST_URLS: set[str] = set()

# Превью видео в /review
PREVIEW_VIDEO_MAX_MB = int(os.getenv("PREVIEW_VIDEO_MAX_MB", "50"))

# Очистка отправленного
CLEANUP_SENT = os.getenv("CLEANUP_SENT", "1").lower() in ("1","true","yes","on")

# Ретраи и терминальный статус (применяется только к отправке; загрузка ссылок теперь вручную)
MAX_SEND_RETRIES     = int(os.getenv("MAX_SEND_RETRIES", "5"))

# Минимальный интервал между постами
MIN_GAP_MINUTES     = int(os.getenv("MIN_GAP_MINUTES", "0"))
GAP_JITTER_MIN_MIN  = int(os.getenv("GAP_JITTER_MIN_MIN", "5"))
GAP_JITTER_MAX_MIN  = int(os.getenv("GAP_JITTER_MAX_MIN", "20"))
APPLY_GAP_TO_FORCE  = os.getenv("APPLY_GAP_TO_FORCE", "0").lower() in ("1","true","yes","on")

LAST_SENT_FILE = (QUEUE_DIR / "last_sent.json")

# Глобальная пауза публикаций
APPLY_PAUSE_TO_FORCE = os.getenv("APPLY_PAUSE_TO_FORCE", "1").lower() in ("1","true","yes","on")
PAUSE_RESCHEDULE_MIN_MIN = int(os.getenv("PAUSE_RESCHEDULE_MIN_MIN", "20"))
PAUSE_RESCHEDULE_MAX_MIN = int(os.getenv("PAUSE_RESCHEDULE_MAX_MIN", "60"))
PAUSE_FILE = (QUEUE_DIR / "pause.json")

# Надёжная запись задач (Windows-friendly)
TASK_WRITE_MAX_RETRIES = int(os.getenv("TASK_WRITE_MAX_RETRIES", "50"))
TASK_WRITE_BASE_SLEEP  = float(os.getenv("TASK_WRITE_BASE_SLEEP", "0.2"))

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
logger = logging.getLogger("vk-yt-bot")


# ==================== Вспомогалки ====================

def is_youtube(url: str) -> bool:
    d = urlparse(url).netloc
    return "youtube.com" in d or "youtu.be" in d

def is_vk(url: str) -> bool:
    d = urlparse(url).netloc
    return ("vk.com" in d) or ("vkvideo.ru" in d) or ("m.vk.com" in d)

def ensure_ffmpeg():
    if shutil.which("ffmpeg") is None:
        raise RuntimeError("ffmpeg не найден в PATH. Установи ffmpeg и добавь в PATH.")

def cookies_from_browser_opt():
    if COOKIES_BROWSER and COOKIES_PROFILE:
        if COOKIES_BROWSER.lower() in {"yandex", "ya", "yabrowser"}:
            raise RuntimeError("Yandex.Браузер не поддерживается cookiesfrombrowser. Используй Chrome/Edge/Firefox.")
        return (COOKIES_BROWSER, COOKIES_PROFILE)
    return None

async def safe_edit(msg_obj: Message, text: str, **kwargs) -> Message:
    if (getattr(msg_obj, "text", None) == text) and ("reply_markup" not in kwargs):
        return msg_obj
    try:
        return await msg_obj.edit_text(text, **kwargs)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            return msg_obj
        raise

def _iter_overdue(now_dt: datetime):
    """Итератор по задачам, у которых run_at < now и статус позволяет отправку."""
    for t in list_tasks():
        st = (t.get("status") or "").upper()
        if st not in ("READY", "SCHEDULED"):
            continue
        ra = t.get("run_at")
        if not ra:
            continue
        try:
            dt = datetime.fromisoformat(ra)
        except Exception:
            continue
        if dt < now_dt:
            # Если это ссылка без подготовленного файла — пропускаем (отправить нечего)
            if t.get("url") and not t.get("video_path"):
                continue
            yield t

def _pick_overdue_batch(limit: int) -> list[dict]:
    now_dt = datetime.now(TZ)
    items = list(_iter_overdue(now_dt))
    if not items:
        return []
    # Старшие (самые ранние по run_at) вперёд
    items.sort(key=lambda x: x.get("run_at") or x.get("created_at") or "")
    return items[:max(1, limit)]

async def send_overdue():
    """Каждые OVERDUE_SCAN_MINUTES забирать до OVERDUE_PER_TICK просроченных задач и пытаться их отправить."""
    batch = _pick_overdue_batch(OVERDUE_PER_TICK)
    if not batch:
        return
    for t in batch:
        tid = t["id"]
        try:
            # Используем существующую логику отправки.
            # Внутри send_task() уже есть проверки окна/паузы/мин-интервала с умным переназначением.
            await send_task(tid)
            logger.info("Overdue sent/handled: %s", tid[:8])
        except Exception as e:
            # Не падаем — просто лог. Статус/ошибка сохранит send_task() или здесь не трогаем.
            logger.error("Overdue send failed for %s: %s", tid[:8], e)

def pick_thumb_ts(duration: float) -> float:
    if duration and duration > 0:
        return max(0.5, min(duration * 0.1, 60.0))
    return 1.0

def _gap_left(now: datetime) -> timedelta | None:
    """Сколько ещё ждать до MIN_GAP_MINUTES (без джиттера)."""
    if MIN_GAP_MINUTES <= 0:
        return None
    last = _read_last_sent_at()
    if not last:
        return None
    left = timedelta(minutes=MIN_GAP_MINUTES) - (now - last)
    return left if left > timedelta(0) else None


def create_task_local_deferred_from_tg(video: "aiogram.types.Video", *, caption: str | None) -> dict:
    now = datetime.now(TZ)
    t = {
        "id": uuid4().hex,
        "type": "video",
        "created_at": now.isoformat(),
        "status": "NEW_LOCAL",              # <- метка отложенной подготовки
        "attempts_send": 0,
        "next_retry_at": now.isoformat(),
        "run_at": pick_run_datetime(now, RANDOM_DAYS_MAX).isoformat(),
        "url": None,
        "video_path": None,
        "photo_path": None,
        "thumb_path": None,
        "duration": int(video.duration or 0),
        "width": video.width or None,
        "height": video.height or None,
        "caption": caption,
        "content_hash": None,
        "last_error": None,
        # telegram source
        "tg_file_id": video.file_id,
        "tg_file_unique_id": video.file_unique_id,
        "tg_mime": video.mime_type,
    }
    save_task(t)
    return t

async def process_local_deferred_task(task_id: str):
    t = load_task(task_id)
    if not t.get("tg_file_id"):
        raise RuntimeError("У задачи нет tg_file_id")

    # 1) скачать файл из TG во временный путь
    ext = ".mp4" if (t.get("tg_mime") or "").endswith("mp4") else ".bin"
    tmp_raw = await _download_to_temp(t["tg_file_id"], ext)
    final = Path(tmp_raw)

    # 2) перекод/обрезка при необходимости
    duration = int(t.get("duration") or 0)
    need_trim = TRIM_TAIL_SEC > 0 and duration > TRIM_TAIL_SEC
    if final.suffix.lower() != ".mp4" or need_trim:
        ensure_ffmpeg()
        out = unique_path(MEDIA_DIR / f"tg_vprep_{task_id}.mp4")
        cmd = ["ffmpeg", "-y", "-i", str(final)]
        if need_trim:
            cmd += ["-to", f"{max(1, duration - int(TRIM_TAIL_SEC))}"]
        cmd += ["-movflags", "+faststart",
                "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
                "-c:a", "aac", "-b:a", "128k", str(out)]
        proc = await asyncio.create_subprocess_exec(*cmd,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        _, err = await proc.communicate()
        if proc.returncode != 0:
            with suppress(Exception): Path(tmp_raw).unlink()
            raise RuntimeError(err.decode(errors="ignore"))
        final = out

    # 3) миниатюра
    thumb = None
    with suppress(Exception):
        thumb_tmp = MEDIA_DIR / f"{final.stem}.jpg"
        ts = pick_thumb_ts(float(duration) if duration else 10.0)
        if await make_thumbnail(final, ts, thumb_tmp):
            thumb = str(thumb_tmp)

    # 4) перенос в постоянное место, хеш
    dst = final
    if dst.parent != MEDIA_DIR:
        dst2 = unique_path(MEDIA_DIR / dst.name)
        shutil.move(str(dst), str(dst2))
        dst = dst2
    with suppress(Exception):
        Path(tmp_raw).unlink()

    t["video_path"] = str(dst)
    t["thumb_path"] = thumb
    with suppress(Exception):
        t["content_hash"] = compute_hash(dst)

    set_status(t, "READY", None)
    # перепланировать отправку (используем уже записанный run_at)
    try:
        schedule_send_at(task_id, datetime.fromisoformat(t["run_at"]))
    except Exception:
        ra = pick_run_datetime(datetime.now(TZ), RANDOM_DAYS_MAX)
        t["run_at"] = ra.isoformat()
        set_status(t, "SCHEDULED", None)
        schedule_send_at(task_id, ra)

def _reschedule_after_resume() -> int:
    """
    Перепланировать READY/SCHEDULED на ближайшее допустимое время
    (учитывая окно и min-gap). Ссылки без подготовки не трогаем.
    Возвращает число поставленных задач.
    """
    now = datetime.now(TZ)
    count = 0
    for t in list_tasks():
        st = (t.get("status") or "").upper()
        if st not in ("READY", "SCHEDULED"):
            continue
        if t.get("url") and not t.get("video_path"):
            # ссылка без подготовки — не планируем
            continue

        # исходное время
        run_at = None
        with suppress(Exception):
            if t.get("run_at"):
                run_at = datetime.fromisoformat(t["run_at"])

        # нужно ли двигать
        if (run_at is None) or (run_at <= now) or (not within_window(run_at)):
            run_at = next_window_start(max(now, run_at or now))
            # учесть минимальный интервал
            delay_gap = _gap_delay_needed_at(run_at)
            if delay_gap:
                run_at = next_window_start(run_at + delay_gap)

            t["run_at"] = run_at.isoformat()
            set_status(t, "SCHEDULED", None)
        # поставить в планировщик (idempotent)
        schedule_send_at(t["id"], run_at)
        count += 1
    return count


async def make_thumbnail(src: Path, ts: float, dst: Path) -> bool:
    if shutil.which("ffmpeg") is None:
        return False
    cmd = [
        "ffmpeg", "-y",
        "-ss", f"{ts:.3f}",
        "-i", str(src),
        "-frames:v", "1",
        "-vf", "scale=320:-2:flags=lanczos",
        "-q:v", "5",
        str(dst)
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, _ = await proc.communicate()
    return proc.returncode == 0 and dst.exists() and dst.stat().st_size > 0

# Человекочитаемая дельта (СТАРАЯ версия — может быть отрицательной)
def human_td(dt: timedelta) -> str:
    total_min = int(dt.total_seconds() // 60)
    sign = "-" if total_min < 0 else ""
    m = abs(total_min)
    h, m = divmod(m, 60)
    if h:
        return f"{sign}{h}ч {m}м"
    return f"{sign}{m}м"

def within_window(dt: datetime) -> bool:
    return POST_WINDOW_START <= dt.hour < POST_WINDOW_END

def pick_run_datetime(now: datetime, days_max: int) -> datetime:
    for _ in range(10):
        day_offset = random.randint(0, max(0, days_max))
        base = (now + timedelta(days=day_offset)).replace(minute=0, second=0, microsecond=0)
        hour = random.randint(POST_WINDOW_START, POST_WINDOW_END - 1)
        minute = random.randint(0, 59)
        cand = base.replace(hour=hour, minute=minute)
        if cand >= now:
            return cand
    nxt = now + timedelta(days=1)
    return nxt.replace(hour=POST_WINDOW_START, minute=0, second=0, microsecond=0)

def backoff_sec(n: int) -> int:
    return min(3600, int(5 * (2 ** max(0, n - 1))))

# Заглушка логгера yt-dlp (чтобы не сыпал в консоль при ошибках)
class _YDLQuietLogger:
    def debug(self, msg): pass
    def info(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): pass

# ===== История =====

def canonical_url(u: str) -> str | None:
    try:
        pr = urlparse(u)
    except Exception:
        return None
    host = pr.netloc.lower()
    path = pr.path
    yt = re.search(r"(?:youtu\.be/|v=|/shorts/|/live/)([A-Za-z0-9_-]{6,})", u)
    if "youtu" in host and yt:
        return f"youtube:{yt.group(1)}"
    if "vk.com" in host or "vkvideo.ru" in host or "m.vk.com" in host:
        return f"vk:{path}"
    return f"{pr.scheme}://{host}{path}"

def history_init():
    HIST_HASHES.clear(); HIST_URLS.clear()
    if not HISTORY_FILE.exists():
        return
    with HISTORY_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            with suppress(Exception):
                obj = json.loads(line)
                h = obj.get("content_hash")
                cu = obj.get("url_canon")
                if h: HIST_HASHES.add(h)
                if cu: HIST_URLS.add(cu)

def history_add(t: dict, *, source: str):
    if not HISTORY_ENABLED:
        return
    rec = {
        "task_id": t.get("id"),
        "type": t.get("type"),
        "sent_at": datetime.now(TZ).isoformat(),
        "source": source,  # auto | manual
    }
    if t.get("content_hash"):
        rec["content_hash"] = t["content_hash"]; HIST_HASHES.add(t["content_hash"])
    if t.get("url"):
        rec["url_raw"] = t["url"]
        rec["url_canon"] = canonical_url(t["url"])
        if rec["url_canon"]:
            HIST_URLS.add(rec["url_canon"])
    if t.get("caption"):
        rec["caption_preview"] = (t["caption"] or "")[:200]

    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with HISTORY_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def history_seen_for_task(t: dict) -> bool:
    if not HISTORY_ENABLED:
        return False
    if t.get("url"):
        cu = canonical_url(t["url"])
        return bool(cu and cu in HIST_URLS)
    h = t.get("content_hash")
    return bool(h and h in HIST_HASHES)

# ===== Хранилище задач =====

def task_path(task_id: str) -> Path:
    return TASKS_DIR / f"{task_id}.json"

def load_task(task_id: str) -> dict:
    with task_path(task_id).open("r", encoding="utf-8") as f:
        return json.load(f)

def list_tasks() -> list[dict]:
    res = []
    for p in TASKS_DIR.glob("*.json"):
        with suppress(Exception):
            with p.open("r", encoding="utf-8") as f:
                res.append(json.load(f))
    return res

def _is_mp4_h264_aac(info, path: Path) -> bool:
    if path.suffix.lower() != ".mp4":
        return False
    v = (info.get("vcodec") or "").lower()
    a = (info.get("acodec") or "").lower()
    return ("avc" in v or "h264" in v) and ("mp4a" in a or "aac" in a)
def save_task(t: dict):
    """Надёжная запись JSON (ретраи; без падений на WinError 5)."""
    tid = t["id"]
    p = task_path(tid)
    p.parent.mkdir(parents=True, exist_ok=True)

    tmp = p.with_suffix(".json.tmp")
    data = json.dumps(t, ensure_ascii=False, indent=2)

    with tmp.open("w", encoding="utf-8") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())

    last_err = None
    for i in range(TASK_WRITE_MAX_RETRIES):
        try:
            tmp.replace(p)
            return
        except PermissionError as e:
            last_err = e
            sleep_s = TASK_WRITE_BASE_SLEEP * (1.2 ** i) + random.uniform(0, 0.05)
            time.sleep(min(sleep_s, 0.5))
            continue
        except Exception as e:
            last_err = e
            break
    logger.warning("save_task: postpone replace for %s (%s)", tid, last_err)

def set_status(t: dict, status: str, err: str | None):
    t["status"] = status
    t["last_error"] = err
    save_task(t)

# ===== Дедуп медиа =====
import hashlib

def compute_hash(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def get_or_fill_task_hash(t: dict) -> str | None:
    h = t.get("content_hash")
    if not h:
        p = t.get("photo_path") or t.get("video_path")
        if p and Path(p).exists():
            with suppress(Exception):
                h = compute_hash(Path(p))
                t["content_hash"] = h
                save_task(t)
    return h

def find_task_by_hash(h: str) -> dict | None:
    for t in list_tasks():
        st = (t.get("status") or "").upper()
        if st == "SENT":
            continue
        if get_or_fill_task_hash(t) == h:
            return t
    return None

async def _download_to_temp(file_id: str, suffix: str) -> Path:
    tg_file = await bot.get_file(file_id)
    tmp = Path(tempfile.gettempdir()) / f"tg_{uuid4().hex}{suffix}"
    await bot.download(tg_file, destination=tmp)
    return tmp

# ===== Пауза публикаций =====

def is_paused() -> bool:
    try:
        if PAUSE_FILE.exists():
            obj = json.loads(PAUSE_FILE.read_text(encoding="utf-8"))
            return bool(obj.get("paused"))
    except Exception:
        pass
    return False

def set_paused(value: bool):
    PAUSE_FILE.parent.mkdir(parents=True, exist_ok=True)
    if value:
        PAUSE_FILE.write_text(json.dumps({"paused": True, "since": datetime.now(TZ).isoformat()}), encoding="utf-8")
        with suppress(Exception):
            for job in scheduler.get_jobs():
                if job.id.startswith("send_"):
                    scheduler.remove_job(job.id)
    else:
        PAUSE_FILE.write_text(json.dumps({"paused": False}), encoding="utf-8")

def pause_since_str() -> str:
    try:
        if PAUSE_FILE.exists():
            obj = json.loads(PAUSE_FILE.read_text(encoding="utf-8"))
            s = obj.get("since")
            if s:
                return datetime.fromisoformat(s).strftime("%Y-%m-%d %H:%M")
    except Exception:
        pass
    return "-"

# ===== Мин. интервал =====

def _read_last_sent_at() -> datetime | None:
    try:
        if LAST_SENT_FILE.exists():
            obj = json.loads(LAST_SENT_FILE.read_text(encoding="utf-8"))
            v = obj.get("last_sent_at")
            if v:
                return datetime.fromisoformat(v)
    except Exception:
        pass
    return None


def _write_last_sent_now():
    try:
        LAST_SENT_FILE.parent.mkdir(parents=True, exist_ok=True)
        LAST_SENT_FILE.write_text(json.dumps({"last_sent_at": datetime.now(TZ).isoformat()}), encoding="utf-8")
    except Exception:
        pass

def _gap_delay_needed(now: datetime) -> timedelta | None:
    if MIN_GAP_MINUTES <= 0:
        return None
    last = _read_last_sent_at()
    if not last:
        return None
    need = timedelta(minutes=MIN_GAP_MINUTES) - (now - last)
    if need <= timedelta(0):
        return None
    jitter_min = max(0, GAP_JITTER_MIN_MIN)
    jitter_max = max(jitter_min, GAP_JITTER_MAX_MIN)
    extra = timedelta(minutes=random.randint(jitter_min, jitter_max))
    return need + extra

# ===== yt-dlp подготовка (вызов ТОЛЬКО вручную) =====

async def ytdlp_prepare(url: str, tmpdir: Path):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    }
    if is_youtube(url):
        headers["Referer"] = "https://www.youtube.com/"
    elif is_vk(url):
        headers["Referer"] = "https://vk.com/"

    ydl_opts = {
        "outtmpl": str(tmpdir / "%(id)s.%(ext)s"),
        "format": (
            "bv*[vcodec^=avc1][ext=mp4]+ba[ext=m4a]/"
            "b[ext=mp4]/"
            "bv*+ba/best"
        ),
        "merge_output_format": "mp4",
        "noplaylist": True,
        "geo_bypass": True,
        "quiet": True,
        "nopart": True,
        "retries": 1,
        "fragment_retries": 1,
        "concurrent_fragment_downloads": 12,
        "socket_timeout": 25,
        "http_headers": headers,
    }

    cfbo = cookies_from_browser_opt()
    if cfbo:
        ydl_opts["cookiesfrombrowser"] = cfbo

    if is_youtube(url) and not (ydl_opts.get("cookiefile") or ydl_opts.get("cookiesfrombrowser")):
        raise RuntimeError("YouTube требует куки. Задай COOKIES_BROWSER+COOKIES_PROFILE или COOKIES_FILE/COOKIES_B64.")

    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        in_path = Path(ydl.prepare_filename(info))

    ext = in_path.suffix.lower()
    image_exts = {".jpg", ".jpeg", ".png", ".webp"}
    width = info.get("width") or None
    height = info.get("height") or None

    # 1) Статика: фото из поста ВК
    if ext in image_exts or (
        (info.get("vcodec") in (None, "none")) and
        (info.get("acodec") in (None, "none")) and
        not info.get("duration")
    ):
        dst = unique_path(MEDIA_DIR / (in_path.stem + ext))
        shutil.move(str(in_path), str(dst))
        return {
            "kind": "photo",
            "media": str(dst),
            "thumb": None,
            "duration": None,
            "width": width,
            "height": height,
        }

    # 2) Анимация gif -> mp4
    if ext == ".gif":
        ensure_ffmpeg()
        out_path = tmpdir / (in_path.stem + ".mp4")
        cmd = [
            "ffmpeg", "-y",
            "-i", str(in_path),
            "-movflags", "+faststart",
            "-pix_fmt", "yuv420p",
            "-c:v", "libx264", "-crf", "23",
            str(out_path)
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, err = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(err.decode(errors="ignore"))
        final_path = out_path
        duration_final = None  # для гиф можно не ставить
    else:
        # 3) Обычное видео
        duration_src = float(info.get("duration") or 0.0)
        tail = max(0.0, TRIM_TAIL_SEC)
        cut_to = max(0.0, duration_src - tail) if duration_src > 0 else 0.0
        need_trim = (tail > 0.0) and (duration_src > 0.0) and (cut_to > 0.0)
        need_convert = (in_path.suffix.lower() != ".mp4") or need_trim

        if need_convert:
            ensure_ffmpeg()
            base = in_path.with_suffix("")
            suffix = f"_trim{int(tail)}" if need_trim else ""
            out_path = base.parent / f"{base.name}{suffix}.mp4"
            cmd = ["ffmpeg", "-y", "-i", str(in_path)]
            if need_trim:
                cmd += ["-to", f"{cut_to:.3f}"]
            cmd += [
                "-movflags", "+faststart",
                "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
                "-c:a", "aac", "-b:a", "128k",
                str(out_path)
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            _, err = await proc.communicate()
            if proc.returncode != 0:
                raise RuntimeError(err.decode(errors="ignore"))
            final_path = out_path
            duration_final = int(max(1, round(cut_to))) if need_trim else int(max(1, round(duration_src)))
        else:
            final_path = in_path
            duration_final = int(max(1, round(duration_src)))

    # Миниатюра (для видео)
    tmp_thumb = tmpdir / "thumb.jpg"
    made_thumb = await make_thumbnail(final_path, pick_thumb_ts((info.get("duration") or 10.0)), tmp_thumb)

    dst_video = unique_path(MEDIA_DIR / final_path.name)
    shutil.move(str(final_path), str(dst_video))
    dst_thumb = None
    if made_thumb and tmp_thumb.exists():
        dst_thumb = unique_path(MEDIA_DIR / (dst_video.stem + ".jpg"))
        shutil.move(str(tmp_thumb), str(dst_thumb))

    return {
        "kind": "video",
        "media": str(dst_video),
        "thumb": (str(dst_thumb) if dst_thumb else None),
        "duration": duration_final,
        "width": width,
        "height": height,
    }


# ===== Утилиты путей =====

def unique_path(target: Path) -> Path:
    if not target.exists():
        return target
    base, ext = target.stem, target.suffix
    i = 1
    while True:
        cand = target.with_name(f"{base}_{i}{ext}")
        if not cand.exists():
            return cand
        i += 1

# ===== Планировщик =====

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=str(TZ))

def schedule_send_at(task_id: str, run_at: datetime):
    job_id = f"send_{task_id}"
    with suppress(Exception):
        scheduler.remove_job(job_id)
    scheduler.add_job(
        send_task,
        trigger='date',
        run_date=run_at,
        id=job_id,
        args=[task_id],
    )
    t = load_task(task_id)
    t["run_at"] = run_at.isoformat()
    if (t.get("status") or "").upper() != "SCHEDULED":
        set_status(t, "SCHEDULED", None)
    else:
        save_task(t)

# ===== Создание задач =====

def create_task_local(kind: str, file_path: str, *,
                      duration: int | None = None,
                      width: int | None = None,
                      height: int | None = None,
                      thumb_path: str | None = None,
                      caption: str | None = None,
                      content_hash: str | None = None) -> dict:
    now = datetime.now(TZ)
    t = {
        "id": uuid4().hex,
        "type": kind,  # "photo" | "video"
        "created_at": now.isoformat(),
        "status": "READY",
        "attempts_send": 0,
        "next_retry_at": now.isoformat(),
        "run_at": pick_run_datetime(now, RANDOM_DAYS_MAX).isoformat(),
        "url": None,
        "video_path": file_path if kind == "video" else None,
        "photo_path": file_path if kind == "photo" else None,
        "thumb_path": thumb_path,
        "duration": duration,
        "width": width,
        "height": height,
        "caption": caption,
        "content_hash": content_hash,
        "last_error": None,
    }
    save_task(t)
    return t

def create_task_url(url: str, caption: str | None = None) -> dict:
    now = datetime.now(TZ)
    t = {
        "id": uuid4().hex,
        "type": "video",
        "created_at": now.isoformat(),
        # ВАЖНО: автоматической обработки больше нет — ждём ручную подготовку
        "status": "NEW",  # NEW = ждёт ручного «🎬 Подготовить» или «🚀 Отправить сейчас»
        "attempts_send": 0,
        "next_retry_at": now.isoformat(),
        "run_at": pick_run_datetime(now, RANDOM_DAYS_MAX).isoformat(),  # время публикации ПОСЛЕ подготовки
        "url": url,
        "video_path": None,
        "photo_path": None,
        "thumb_path": None,
        "duration": None,
        "width": None,
        "height": None,
        "caption": caption,
        "content_hash": None,
        "last_error": None,
    }
    save_task(t)
    return t

# ===== Восстановление при старте =====

def restore_on_start():
    now = datetime.now(TZ)
    for t in list_tasks():
        tid = t["id"]
        status = (t.get("status") or "").upper()
        try:
            if status in ("SENT", "FAILED"):
                continue

            # Для ссылок: НИЧЕГО не делаем автоматически — ждём ручной подготовки
            if t.get("url") and not t.get("video_path"):
                # statuses: NEW|ERROR — оставляем как есть
                continue

            # Для локальных фото/видео: отправка по расписанию
            if status in ("READY", "SCHEDULED"):
                run_at = t.get("run_at")
                try:
                    run_at = datetime.fromisoformat(run_at) if run_at else pick_run_datetime(now, RANDOM_DAYS_MAX)
                except Exception:
                    run_at = pick_run_datetime(now, RANDOM_DAYS_MAX)
                schedule_send_at(tid, run_at)
                continue

            if status in ("ERROR",):
                # Ошибка отправки локального медиа: авто-ретрай по next_retry_at
                if not t.get("url"):  # только для локальных
                    try:
                        nra = datetime.fromisoformat(t.get("next_retry_at") or "")
                    except Exception:
                        nra = now + timedelta(seconds=backoff_sec(1))
                    delay = max(1, int((nra - now).total_seconds()))
                    schedule_send_at(tid, now + timedelta(seconds=delay))
                # для ссылок — ничего не делаем (ручной режим)
                continue

            # Неизвестный статус — игнор
        except Exception:
            logger.exception("restore_on_start error for task %s", tid)

# ==================== Отправка/процессинг ====================

async def process_task(task_id: str):
    t = load_task(task_id)
    if not t.get("url"):
        return
    url = t["url"]
    try:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            res = await ytdlp_prepare(url, tmpdir)

            if res["kind"] == "photo":
                t["type"] = "photo"
                t["photo_path"] = res["media"]
                t["video_path"] = None
                t["thumb_path"] = None
                t["duration"] = None
                t["width"] = res.get("width")
                t["height"] = res.get("height")
            else:
                t["type"] = "video"
                t["video_path"] = res["media"]
                t["thumb_path"] = res.get("thumb")
                t["duration"] = res.get("duration")
                t["width"] = res.get("width")
                t["height"] = res.get("height")

            with suppress(Exception):
                content_path = t.get("photo_path") or t.get("video_path")
                if content_path:
                    t["content_hash"] = compute_hash(Path(content_path))

            set_status(t, "READY", None)
            run_at = datetime.fromisoformat(t["run_at"])
            schedule_send_at(task_id, run_at)

    except Exception as e:
        msg_txt = (str(e) or "Unknown error")[:500]
        t["attempts_download"] = int(t.get("attempts_download") or 0) + 1
        set_status(t, "ERROR", msg_txt)

def resolve_chat_id(fallback: int | str | None = None):
    if CHANNEL_ID is not None:
        return CHANNEL_ID
    if fallback is not None:
        return fallback
    raise RuntimeError("CHANNEL_ID не задан. Укажи @username или -100... в .env")

async def send_task(task_id: str):
    t = load_task(task_id)
    now = datetime.now(TZ)

    # окно публикаций
    if not within_window(now):
        new_dt = pick_run_datetime(now, RANDOM_DAYS_MAX)
        t["run_at"] = new_dt.isoformat()
        set_status(t, "SCHEDULED", "Вне окна публикаций")
        schedule_send_at(task_id, new_dt)
        return

    # глобальная пауза
    if is_paused():
        jitter = random.randint(PAUSE_RESCHEDULE_MIN_MIN, PAUSE_RESCHEDULE_MAX_MIN)
        new_dt = now + timedelta(minutes=jitter)
        if not within_window(new_dt):
            new_dt = pick_run_datetime(new_dt, RANDOM_DAYS_MAX)
        t["run_at"] = new_dt.isoformat()
        set_status(t, "SCHEDULED", "Пауза публикаций включена")
        schedule_send_at(task_id, new_dt)
        return

    # минимальный интервал
    delay_gap = _gap_delay_needed(now)
    if delay_gap:
        new_dt = now + delay_gap
        if not within_window(new_dt):
            new_dt = pick_run_datetime(new_dt, RANDOM_DAYS_MAX)
        t["run_at"] = new_dt.isoformat()
        set_status(t, "SCHEDULED", f"Мин. интервал {MIN_GAP_MINUTES} мин")
        schedule_send_at(task_id, new_dt)
        return

    # защита: ссылки без подготовки отправлять нельзя
    if t.get("url") and not t.get("video_path"):
        set_status(t, "ERROR", "Ссылка не подготовлена. Нажмите 🎬 Подготовить или /process <id>.")
        return

    # автопостинг только в канал
    if CHANNEL_ID is None:
        set_status(t, "ERROR", "CHANNEL_ID не задан. Укажи @username или -100... в .env")
        schedule_send_at(task_id, now + timedelta(minutes=15))
        return

    # отправка
    kind = t.get("type") or "video"
    caption = (t.get("caption") or None)
    if caption:
        caption = caption[:1024]
    try:
        if kind == "photo":
            kwargs = dict(chat_id=CHANNEL_ID, photo=FSInputFile(t["photo_path"]))
            if caption:
                kwargs["caption"] = caption
            await bot.send_photo(**kwargs)
        else:
            kwargs = dict(chat_id=CHANNEL_ID, video=FSInputFile(t["video_path"]), supports_streaming=True)
            if t.get("duration"):
                kwargs["duration"] = int(t["duration"])
            if t.get("width") and t.get("height"):
                kwargs.update(width=int(t["width"]), height=int(t["height"]))
            if t.get("thumb_path") and Path(t["thumb_path"]).exists():
                kwargs.update(thumbnail=FSInputFile(t["thumb_path"]))
            if caption:
                kwargs["caption"] = caption
            await bot.send_video(**kwargs)

        set_status(t, "SENT", None)
        history_add(t, source="auto")
        _write_last_sent_now()

        if CLEANUP_SENT:
            with suppress(Exception):
                vp = t.get("video_path");  pp = t.get("photo_path"); tp = t.get("thumb_path")
                if vp: Path(vp).unlink()
                if pp: Path(pp).unlink()
                if tp: Path(tp).unlink()
            with suppress(Exception):
                task_path(task_id).unlink()
        with suppress(Exception):
            scheduler.remove_job(f"send_{task_id}")
    except Exception as e:
        t["attempts_send"] = int(t.get("attempts_send") or 0) + 1
        attempts = t["attempts_send"]
        if attempts >= MAX_SEND_RETRIES:
            set_status(t, "FAILED", f"Ошибка отправки (исчерпаны попытки): {e}")
            logger.warning("Task %s FAILED on send after %s attempts", t["id"], attempts)
            return
        delay = backoff_sec(attempts)
        set_status(t, "ERROR", f"Ошибка отправки: {e}")
        schedule_send_at(task_id, now + timedelta(seconds=delay))

async def send_task_force(task_id: str, target_chat_id: int | str | None = None):
    if is_paused() and APPLY_PAUSE_TO_FORCE:
        raise RuntimeError("Пауза публикаций включена (/resume для продолжения).")
    if APPLY_GAP_TO_FORCE:
        delay_gap = _gap_delay_needed(datetime.now(TZ))
        if delay_gap:
            raise RuntimeError("Минимальный интервал не выдержан. Подождите ещё немного.")

    t = load_task(task_id)

    # Если это ссылка и не подготовлено — СЕЙЧАС подготовим вручную
    if t.get("url") and not t.get("video_path"):
        with tempfile.TemporaryDirectory() as tmp:
            video_path, thumb_path, duration_final, width, height = await ytdlp_prepare(t["url"], Path(tmp))
        t["video_path"] = video_path
        t["thumb_path"] = thumb_path
        t["duration"] = duration_final
        t["width"] = width
        t["height"] = height
        with suppress(Exception):
            t["content_hash"] = compute_hash(Path(video_path))
        set_status(t, "READY", None)

    chat_id = resolve_chat_id(target_chat_id)
    kind = t.get("type") or "video"
    caption = (t.get("caption") or None)
    if caption:
        caption = caption[:1024]

    if kind == "photo":
        kwargs = dict(chat_id=chat_id, photo=FSInputFile(t["photo_path"]))
        if caption:
            kwargs["caption"] = caption
        await bot.send_photo(**kwargs)
    else:
        kwargs = dict(chat_id=chat_id, video=FSInputFile(t["video_path"]), supports_streaming=True)
        if t.get("duration"):
            kwargs["duration"] = int(t["duration"])
        if t.get("width") and t.get("height"):
            kwargs.update(width=int(t["width"]), height=int(t["height"]))
        if t.get("thumb_path") and Path(t["thumb_path"]).exists():
            kwargs.update(thumbnail=FSInputFile(t["thumb_path"]))
        if caption:
            kwargs["caption"] = caption
        await bot.send_video(**kwargs)

    set_status(t, "SENT", None)
    history_add(t, source="manual")
    _write_last_sent_now()

    with suppress(Exception):
        scheduler.remove_job(f"send_{task_id}")
    if CLEANUP_SENT:
        with suppress(Exception):
            vp = t.get("video_path");  pp = t.get("photo_path"); tp = t.get("thumb_path")
            if vp: Path(vp).unlink()
            if pp: Path(pp).unlink()
            if tp: Path(tp).unlink()
        with suppress(Exception):
            task_path(task_id).unlink()

# ==================== Хэндлеры ====================

@dp.message(CommandStart())
async def start(msg: Message):
    await msg.reply(
        "Кинь ссылку на VK или YouTube (включая Shorts), фото или видео.\n"
        f"Окно постинга: {POST_WINDOW_START}:00–{POST_WINDOW_END}:00. "
        f"Мин. интервал: {MIN_GAP_MINUTES} мин.\n"
        "Ссылки теперь обрабатываются ТОЛЬКО вручную: /review → 🎬 Подготовить или /process <id>."
    )

# ===== Приём ссылок =====
VK_LINK_RE = r"https?://(?:m\.)?(?:vk\.com|vkvideo\.ru)/\S+"
YT_LINK_RE = r"https?://(?:www\.)?(?:youtube\.com/(?:shorts/|watch\?v=|live/)\S*|youtu\.be/\S+)"
ANY_LINK_RE = rf"(?:{VK_LINK_RE})|(?:{YT_LINK_RE})"

@dp.message(F.text.regexp(ANY_LINK_RE))
async def handle_link(msg: Message):
    url = msg.text.strip()
    note = await msg.reply("Создаю задачу…")
    try:
        # 1) Создаём задачу-ссылку (без файлов)
        t = create_task_url(url, caption=None)

        # 2) Одна автоматическая попытка подготовки
        try:
            await safe_edit(note, "Пробую подготовить (одна попытка)…")
            with tempfile.TemporaryDirectory() as tmp:
                tmpdir = Path(tmp)
                video_path, thumb_path, duration_final, width, height = await ytdlp_prepare(url, tmpdir)

            # успешная подготовка → заполняем поля
            t["video_path"] = video_path
            t["thumb_path"] = thumb_path
            t["duration"] = duration_final
            t["width"] = width
            t["height"] = height
            with suppress(Exception):
                t["content_hash"] = compute_hash(Path(video_path))

            set_status(t, "READY", None)

            # назначаем отправку на уже выбранный run_at
            run_at = datetime.fromisoformat(t["run_at"])
            schedule_send_at(t["id"], run_at)

            eta = human_td(run_at - datetime.now(TZ))
            await safe_edit(
                note,
                f"✅ Подготовлено.\n"
                f"id: {t['id'][:8]}\n"
                f"Запланировано: {run_at:%Y-%m-%d %H:%M} ({eta}).",
                disable_web_page_preview=True
            )
            await _render_one_review(msg.chat.id, "links", t)

        except Exception as e:
            # не удалось подготовить — оставляем задачу «неподготовленной»
            err = (str(e) or "Unknown error")[:500]
            t["last_error"] = err
            set_status(t, "NEW", err)  # статус NEW = ждёт ручной подготовки

            await safe_edit(
                note,
                "⚠️ Ссылка добавлена, но автоподготовка не удалась.\n"
                f"id: {t['id'][:8]}\n"
                "Подготовь вручную: /review_unprep → 🎬, или /process <id>, или /prepare_all.\n"
                f"Причина: {err}",
                disable_web_page_preview=True
            )
            await _render_one_review(msg.chat.id, "unprep", t)

    except Exception as e:
        await safe_edit(note, f"Ошибка: {e}", disable_web_page_preview=True)

# ===== Приём фото =====
@dp.message(F.photo)
async def handle_photo(msg: Message):
    if CHANNEL_ID is None:
        await msg.reply("Укажи CHANNEL_ID в .env")
        return
    note = await msg.reply("Принял фото, проверяю…")
    tmp = None
    try:
        ph = msg.photo[-1]
        tmp = await _download_to_temp(ph.file_id, ".jpg")
        h = compute_hash(tmp)
        dup = find_task_by_hash(h)
        if dup:
            await safe_edit(note, "⚠️ Такое фото уже в очереди.")
            return
        dst = unique_path(MEDIA_DIR / f"tg_photo_{msg.message_id}.jpg")
        shutil.move(str(tmp), str(dst)); tmp = None
        t = create_task_local("photo", str(dst), caption=msg.caption or None, content_hash=h)
        schedule_send_at(t["id"], datetime.fromisoformat(t["run_at"]))
        await safe_edit(note, f"Фото в очереди. Выйдет через {human_td(datetime.fromisoformat(t['run_at']) - datetime.now(TZ))}.")
        await _render_one_review(msg.chat.id, "media", t)

    except Exception as e:
        await safe_edit(note, f"Ошибка: {e}")
    finally:
        if tmp:
            with suppress(Exception):
                Path(tmp).unlink()

# ===== Приём видео =====
@dp.message(F.video)
async def handle_video(msg: Message):
    note = await msg.reply("Принял видео, подготавливаю…")
    tmp_raw = None
    if DELAY_LOCAL_PREP:
        # не скачиваем сейчас — создаём отложенную задачу
        t = create_task_local_deferred_from_tg(msg.video, caption=msg.caption or None)
        await msg.reply(
            f"Видео поставлено в очередь подготовки. id: {t['id'][:8]}\n"
            "Нажми 🎬 в /review_unprep, или /process <id>, или /prepare_all.",
            disable_web_page_preview=True
        )
        await _render_one_review(msg.chat.id, "unprep", t)
        return
    else:
        try:
            v = msg.video
            ext = ".mp4" if (v.mime_type or "").endswith("mp4") else ".bin"
            tmp_raw = await _download_to_temp(v.file_id, ext)
            final = Path(tmp_raw)
            duration = int(v.duration or 0); width = v.width or None; height = v.height or None
            tail = max(0.0, TRIM_TAIL_SEC)
            need_trim = tail > 0 and duration > tail
            if final.suffix.lower() != ".mp4" or need_trim:
                ensure_ffmpeg()
                out = unique_path(MEDIA_DIR / f"tg_video_{msg.message_id}_prep.mp4")
                cmd = ["ffmpeg", "-y", "-i", str(final)]
                if need_trim:
                    cmd += ["-to", f"{max(1, duration - int(tail))}"]
                cmd += ["-movflags", "+faststart",
                        "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
                        "-c:a", "aac", "-b:a", "128k", str(out)]
                proc = await asyncio.create_subprocess_exec(*cmd,
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                _, err = await proc.communicate()
                if proc.returncode != 0:
                    raise RuntimeError(err.decode(errors="ignore"))
                final = out
            h = compute_hash(final)
            dup = find_task_by_hash(h)
            if dup:
                with suppress(Exception):
                    Path(final).unlink()
                await safe_edit(note, "⚠️ Такое видео уже в очереди.")
                return
            thumb = None
            with suppress(Exception):
                thumb_tmp = MEDIA_DIR / f"{Path(final).stem}.jpg"
                ts = pick_thumb_ts(float(duration) if duration else 10.0)
                if await make_thumbnail(Path(final), ts, thumb_tmp):
                    thumb = str(thumb_tmp)
            dst = Path(final)
            if dst.parent != MEDIA_DIR:
                dst2 = unique_path(MEDIA_DIR / dst.name)
                shutil.move(str(dst), str(dst2))
                dst = dst2
            t = create_task_local("video", str(dst),
                                  duration=duration if duration else None,
                                  width=width, height=height,
                                  thumb_path=thumb,
                                  caption=msg.caption or None,
                                  content_hash=h)
            schedule_send_at(t["id"], datetime.fromisoformat(t["run_at"]))
            await safe_edit(note, f"Видео в очереди. Выйдет через {human_td(datetime.fromisoformat(t['run_at']) - datetime.now(TZ))}.")
            await _render_one_review(msg.chat.id, "media", t)
        except Exception as e:
            await safe_edit(note, f"Ошибка: {e}")
        finally:
            if tmp_raw:
                with suppress(Exception):
                    Path(tmp_raw).unlink()

@dp.message(F.text.startswith(("/review_id", "/reviewid", "/rv")))
async def review_by_id_cmd(msg: Message):
    parts = msg.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await msg.reply("Использование: /review_id <id или префикс>")
        return
    key = parts[1].strip().lower()

    t = None
    for x in list_tasks():
        xid = x["id"]
        if xid == key or xid.startswith(key):
            t = x
            break
    if not t:
        await msg.reply("Задача не найдена.")
        return

    # выбираем режим для корректных кнопок
    if t.get("url") and not t.get("video_path"):
        mode = "unprep"   # ссылка без подготовки
    elif t.get("url"):
        mode = "links"
    else:
        mode = "media"

    await _render_one_review(msg.chat.id, mode, t)


# Дополнительно: позволим писать прямо /review <id>
@dp.message(F.text.startswith("/review "))
async def review_with_id_cmd(msg: Message):
    # переиспользуем логику выше
    fake = Message.model_validate(msg.model_dump())  # копия для безопасности
    fake.text = msg.text.replace("/review", "/review_id", 1).strip()
    await review_by_id_cmd(fake)


# ===== Документы (image/video) =====
@dp.message(F.document)
async def handle_document(msg: Message):
    doc = msg.document
    mt = (doc.mime_type or "").lower()
    if not (mt.startswith("image/") or mt.startswith("video/")):
        return
    if mt.startswith("image/"):
        note = await msg.reply("Принял изображение, проверяю…")
        tmp = None
        try:
            ext = Path(doc.file_name or "image.jpg").suffix or ".jpg"
            tmp = await _download_to_temp(doc.file_id, ext)
            h = compute_hash(tmp)
            dup = find_task_by_hash(h)
            if dup:
                await safe_edit(note, "⚠️ Такая картинка уже в очереди.")
                return
            dst = unique_path(MEDIA_DIR / f"tg_image_{msg.message_id}{ext}")
            shutil.move(str(tmp), str(dst)); tmp = None
            t = create_task_local("photo", str(dst), caption=msg.caption or doc.caption or None, content_hash=h)
            schedule_send_at(t["id"], datetime.fromisoformat(t["run_at"]))
            await safe_edit(note, f"Картинка в очереди. Выйдет через {human_td(datetime.fromisoformat(t['run_at']) - datetime.now(TZ))}.")
            await _render_one_review(msg.chat.id, "media", t)
        except Exception as e:
            await safe_edit(note, f"Ошибка: {e}")
        finally:
            if tmp:
                with suppress(Exception):
                    Path(tmp).unlink()
        return
    if mt.startswith("video/"):
        note = await msg.reply("Принял видео, подготавливаю…")
        tmp_raw = None
        try:
            ext = Path(doc.file_name or "video.mp4").suffix or ".mp4"
            tmp_raw = await _download_to_temp(doc.file_id, ext)
            final = Path(tmp_raw)
            if final.suffix.lower() != ".mp4":
                ensure_ffmpeg()
                out = unique_path(MEDIA_DIR / f"tg_vdoc_{msg.message_id}_prep.mp4")
                cmd = ["ffmpeg", "-y", "-i", str(final),
                       "-movflags", "+faststart",
                       "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
                       "-c:a", "aac", "-b:a", "128k", str(out)]
                proc = await asyncio.create_subprocess_exec(*cmd,
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                _, err = await proc.communicate()
                if proc.returncode != 0:
                    raise RuntimeError(err.decode(errors="ignore"))
                final = out
            h = compute_hash(final)
            dup = find_task_by_hash(h)
            if dup:
                with suppress(Exception):
                    Path(final).unlink()
                await safe_edit(note, "⚠️ Такое видео уже в очереди.")
                return
            thumb = None
            with suppress(Exception):
                thumb_tmp = MEDIA_DIR / f"{Path(final).stem}.jpg"
                if await make_thumbnail(Path(final), 1.0, thumb_tmp):
                    thumb = str(thumb_tmp)
            dst = Path(final)
            if dst.parent != MEDIA_DIR:
                dst2 = unique_path(MEDIA_DIR / dst.name)
                shutil.move(str(dst), str(dst2))
                dst = dst2
            t = create_task_local("video", str(dst),
                                  thumb_path=thumb,
                                  caption=msg.caption or doc.caption or None,
                                  content_hash=h)
            schedule_send_at(t["id"], datetime.fromisoformat(t["run_at"]))
            await safe_edit(note, f"Видео в очереди. Выйдет через {human_td(datetime.fromisoformat(t['run_at']) - datetime.now(TZ))}.")
            await _render_one_review(msg.chat.id, "media", t)
        except Exception as e:
            await safe_edit(note, f"Ошибка: {e}")
        finally:
            if tmp_raw:
                with suppress(Exception):
                    Path(tmp_raw).unlink()

# ===== Очереди =====

MAX_TG_MSG = 4096
async def reply_chunked(msg: Message, text: str):
    if not text:
        await msg.reply("Пусто.", disable_web_page_preview=True)
        return
    chunk = 4000
    for i in range(0, len(text), chunk):
        await msg.reply(text[i:i+chunk], disable_web_page_preview=True)

def _iter_queue(filter_mode: str):
    """
    filter_mode: links | media | all
    """
    now = datetime.now(TZ)
    items = []
    for t in list_tasks():
        st = (t.get("status") or "").upper()
        if st in ("SENT",):
            continue
        is_link = bool(t.get("url"))
        if filter_mode == "links" and not is_link:
            continue
        if filter_mode == "media" and is_link:
            continue
        ra = t.get("run_at")
        try:
            dt = datetime.fromisoformat(ra) if ra else None
        except Exception:
            dt = None
        items.append((t, dt))
    items.sort(key=lambda x: x[1] or datetime.max.replace(tzinfo=TZ))
    lines = []
    for t, dt in items:
        title = t.get("url") or Path(t.get("video_path") or t.get("photo_path") or "").name or "—"
        status = (t.get("status") or "").upper()
        eta = human_td((dt - now) if dt else timedelta(0))
        when = dt.strftime("%Y-%m-%d %H:%M") if dt else "—"
        lines.append(f"- {title} — {status} — через {eta} (в {when})")
    return "\n".join(lines) if lines else "Пусто."

@dp.message(F.text == "/queue")
async def queue_cmd(msg: Message):
    await reply_chunked(msg, _iter_queue("links"))

@dp.message(F.text == "/queue_media")
async def queue_media_cmd(msg: Message):
    await reply_chunked(msg, _iter_queue("media"))

@dp.message(F.text == "/queue_all")
async def queue_all_cmd(msg: Message):
    await reply_chunked(msg, _iter_queue("all"))

# ===== Review (просмотр с кнопками) =====

def _filter_tasks_for_review(mode: str) -> list[dict]:
    allow_status = {"NEW", "READY", "SCHEDULED"} if mode != "all" else None
    items = []
    for t in list_tasks():
        st = (t.get("status") or "").upper()

        if mode == "unprep":
            is_unprepped_link = t.get("url") and not t.get("video_path")
            is_unprepped_local = t.get("tg_file_id") and not t.get("video_path")
            if not (is_unprepped_link or is_unprepped_local):
                continue
            if st not in ("NEW", "ERROR", "NEW_LOCAL"):
                continue
        else:
            if allow_status and st not in allow_status:
                continue
            is_link = bool(t.get("url"))
            if mode == "links" and not is_link:
                continue
            if mode == "media" and is_link:
                continue

        items.append(t)

    def _key(t):
        # для unprep сортируем по created_at, иначе — по run_at
        if mode == "unprep":
            ca = t.get("created_at")
            with suppress(Exception):
                return datetime.fromisoformat(ca) if ca else datetime.max.replace(tzinfo=TZ)
        ra = t.get("run_at")
        with suppress(Exception):
            return datetime.fromisoformat(ra) if ra else datetime.max.replace(tzinfo=TZ)
        return datetime.max.replace(tzinfo=TZ)

    items.sort(key=_key)
    return items


def _review_index(mode: str, task_id: str) -> int:
    items = _filter_tasks_for_review(mode)
    for i, t in enumerate(items):
        if t["id"] == task_id:
            return i
    return -1

def _kb_review(mode: str, t: dict) -> InlineKeyboardMarkup:
    nav = [
        InlineKeyboardButton(text="◀️ Пред", callback_data=f"r:{mode}:prev:{t['id']}"),
        InlineKeyboardButton(text="🚀 Отправить сейчас", callback_data=f"r:{mode}:send:{t['id']}"),
        InlineKeyboardButton(text="▶️ След", callback_data=f"r:{mode}:next:{t['id']}")
    ]
    # показываем "🎬 Подготовить", если ссылка/локальное видео ещё не подготовлены
    need_prep = ((t.get("url") and not t.get("video_path")) or
                 (t.get("tg_file_id") and not t.get("video_path")))
    if need_prep:
        nav.insert(1, InlineKeyboardButton(text="🎬 Подготовить", callback_data=f"r:{mode}:prep:{t['id']}"))

    rows = [
        nav,
        [
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"r:{mode}:del:{t['id']}"),
            InlineKeyboardButton(text="❌ Закрыть", callback_data=f"r:{mode}:close:{t['id']}")
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def _render_one_review(chat_id: int, mode: str, t: dict) -> None:
    kind = t.get("type") or "video"
    run_at = t.get("run_at") or ""
    flag = " ⚠️ Уже в истории" if history_seen_for_task(t) else ""
    text_tail = f"Запланировано: {run_at}{flag}\nСтатус: {t.get('status')}"
    kb = _kb_review(mode, t)

    if kind == "photo" and t.get("photo_path") and Path(t["photo_path"]).exists():
        await bot.send_photo(chat_id, photo=FSInputFile(t["photo_path"]), caption=text_tail, reply_markup=kb)
        return
    if kind == "video":
        if t.get("url") and not t.get("video_path"):
            await bot.send_message(chat_id, f"Видео (ссылка): {t['url']}\n{text_tail}",
                                   reply_markup=kb, disable_web_page_preview=True)
            return
        vpath = t.get("video_path")
        if vpath and Path(vpath).exists():
            size_mb = 0.0
            with suppress(Exception):
                size_mb = Path(vpath).stat().st_size / (1024 * 1024)
            if size_mb <= PREVIEW_VIDEO_MAX_MB:
                kwargs = dict(chat_id=chat_id,
                              video=FSInputFile(vpath),
                              supports_streaming=True,
                              caption=text_tail,
                              reply_markup=kb)
                if t.get("duration"):
                    kwargs["duration"] = int(t["duration"])
                await bot.send_video(**kwargs)
                return
            if t.get("thumb_path") and Path(t["thumb_path"]).exists():
                note = f"Видео ({size_mb:.1f} МБ) — превью\n" + text_tail
                await bot.send_photo(chat_id, photo=FSInputFile(t["thumb_path"]), caption=note, reply_markup=kb)
                return
            fname = Path(vpath).name
            await bot.send_message(chat_id, f"Видео: {fname} ({size_mb:.1f} МБ)\n{text_tail}", reply_markup=kb)
            return
        await bot.send_message(chat_id, f"Видео: (файл недоступен)\n{text_tail}", reply_markup=kb)
        return
    if t.get("url"):
        await bot.send_message(chat_id, f"Ссылка: {t['url']}\n{text_tail}", reply_markup=kb, disable_web_page_preview=True)
    else:
        await bot.send_message(chat_id, f"Задача {t['id'][:6]}\n{text_tail}", reply_markup=kb)

@dp.message(F.text == "/review")
async def review_links_cmd(msg: Message):
    items = _filter_tasks_for_review("links")
    if not items:
        await msg.reply("Нет задач по ссылкам.")
        return
    await _render_one_review(msg.chat.id, "links", items[0])

@dp.message(F.text == "/pause")
async def pause_cmd(msg: Message):
    if is_paused():
        await msg.reply(f"Пауза уже включена (с {pause_since_str()}).")
        return
    set_paused(True)
    await msg.reply("⏸ Пауза публикаций включена. Все запланированные отправки остановлены.")


@dp.message(F.text == "/resume")
async def resume_cmd(msg: Message):
    if not is_paused():
        await msg.reply("Пауза не активна.")
        return
    set_paused(False)
    n = _reschedule_after_resume()
    await msg.reply(f"▶️ Пауза снята. Перепланировал задач: {n}.\n"
                    "⚠️ Ссылки без подготовки НЕ запускаются автоматически.")


@dp.message(F.text == "/status")
async def status_cmd(msg: Message):
    now = datetime.now(TZ)
    paused = is_paused()
    next_dt = earliest_run_at()
    last_dt = _read_last_sent_at()
    gap_left = _gap_left(now)

    # счётчики по очереди (без SENT)
    total = 0
    by_status = {}
    by_kind = {"photo": 0, "video": 0}
    for t in list_tasks():
        st = (t.get("status") or "").upper()
        if st == "SENT":
            continue
        total += 1
        by_status[st] = by_status.get(st, 0) + 1
        k = (t.get("type") or "video").lower()
        by_kind[k] = by_kind.get(k, 0) + 1

    lines = [
        f"Пауза: {'включена' if paused else 'выключена'}" + (f" (с {pause_since_str()})" if paused else ""),
        f"Всего в очереди: {total} | фото: {by_kind.get('photo',0)} | видео: {by_kind.get('video',0)}",
        "По статусам: " + ", ".join(f"{k}:{v}" for k, v in sorted(by_status.items())),
        f"Мин. интервал: {MIN_GAP_MINUTES} мин" + (f" (осталось ~{human_td(gap_left)})" if gap_left else ""),
        f"Последний пост: {last_dt.strftime('%Y-%m-%d %H:%M') if last_dt else '—'}",
        f"Следующий пост: {next_dt.strftime('%Y-%m-%d %H:%M') if next_dt else '—'}",
        f"Окно публикаций: {POST_WINDOW_START}:00–{POST_WINDOW_END}:00",
    ]
    await msg.reply("\n".join(lines), disable_web_page_preview=True)

@dp.message(F.text == "/review_media")
async def review_media_cmd(msg: Message):
    items = _filter_tasks_for_review("media")
    if not items:
        await msg.reply("Нет медиа-задач.")
        return
    await _render_one_review(msg.chat.id, "media", items[0])

@dp.message(F.text == "/review_all")
async def review_all_cmd(msg: Message):
    items = _filter_tasks_for_review("all")
    if not items:
        await msg.reply("Нет задач.")
        return
    await _render_one_review(msg.chat.id, "all", items[0])

@dp.callback_query(F.data.startswith("r:"))
async def review_cb(cb):
    try:
        _, mode, action, task_id = cb.data.split(":", 3)
    except Exception:
        await cb.answer("Некорректная команда", show_alert=True)
        return
    items = _filter_tasks_for_review(mode)
    idx = _review_index(mode, task_id)
    if idx == -1 and items:
        idx = 0

    async def _show(i: int):
        await _render_one_review(cb.message.chat.id, mode, items[i])

    if action == "close":
        with suppress(Exception):
            await cb.message.delete()
        await cb.answer("Закрыто")
        return

    if action == "prep":
        try:
            await cb.answer("Готовлю…")
            # запомним позицию до подготовки
            items_before = _filter_tasks_for_review(mode)
            idx_before = _review_index(mode, task_id)
            await process_task(task_id)
            await bot.send_message(cb.message.chat.id, "🎬 Подготовка завершена. Задача переведена в READY.")

            # если идём по неподготовленным — сразу показать следующий
            if mode == "unprep":
                items_after = _filter_tasks_for_review(mode)
                if items_after:
                    i = idx_before if 0 <= idx_before < len(items_after) else 0
                    await _render_one_review(cb.message.chat.id, mode, items_after[i])
                else:
                    await bot.send_message(cb.message.chat.id, "Неподготовленных ссылок больше нет ✅")
        except Exception as e:
            await bot.send_message(cb.message.chat.id, f"❌ Ошибка подготовки: {e}")
        return

    if action == "send":
        t = load_task(task_id)
        if t.get("url") and not t.get("video_path"):
            # если ссылка — отправка сейчас включает ручную подготовку
            pass
        if history_seen_for_task(t):
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="✅ Отправить", callback_data=f"r:{mode}:sendok:{task_id}"),
                InlineKeyboardButton(text="✋ Отмена", callback_data=f"r:{mode}:noop:{task_id}")
            ]])
            await bot.send_message(cb.message.chat.id, "⚠️ Этот мем уже был в истории. Отправить ещё раз?", reply_markup=kb)
            await cb.answer()
            return
        await cb.answer("Отправляю…")
        try:
            await send_task_force(task_id, target_chat_id=cb.message.chat.id)
            await bot.send_message(cb.message.chat.id, "✅ Отправлено.")
        except Exception as e:
            await bot.send_message(cb.message.chat.id, f"❌ Ошибка отправки: {e}")
        return

    if action == "sendok":
        await cb.answer("Отправляю…")
        try:
            await send_task_force(task_id, target_chat_id=cb.message.chat.id)
            await bot.send_message(cb.message.chat.id, "✅ Отправлено.")
        except Exception as e:
            await bot.send_message(cb.message.chat.id, f"❌ Ошибка отправки: {e}")
        return

    if action == "noop":
        await cb.answer("Отменено")
        return

    if not items:
        await cb.answer("Нет задач", show_alert=True)
        return

    if action == "next":
        i = (idx + 1) % len(items)
        await _show(i)
        await cb.answer()
        return

    if action == "prev":
        i = (idx - 1) % len(items)
        await _show(i)
        await cb.answer()
        return
    if action == "del":
        try:
            # удалить задачу и файлы (функция уже есть)
            delete_task(task_id)
            await cb.answer("Удалено")

            # обновить список и показать следующую карточку (если есть)
            items_after = _filter_tasks_for_review(mode)
            if items_after:
                # сохраняем позицию: берём тот же индекс, но не выходим за границы
                i = idx if 0 <= idx < len(items_after) else (len(items_after) - 1)
                await _render_one_review(cb.message.chat.id, mode, items_after[i])
            else:
                await bot.send_message(cb.message.chat.id, "Список пуст.")
        except Exception as e:
            await cb.answer("Ошибка", show_alert=True)
            await bot.send_message(cb.message.chat.id, f"Не удалось удалить: {e}")
        return
    await cb.answer("Ок")

# ===== Ошибки =====

def delete_task(task_id: str) -> None:
    with suppress(Exception):
        scheduler.remove_job(f"send_{task_id}")
    t = load_task(task_id)
    for key in ("video_path", "photo_path", "thumb_path"):
        p = t.get(key)
        if p:
            with suppress(Exception):
                Path(p).unlink()
    with suppress(Exception):
        task_path(task_id).unlink()

async def retry_task(task_id: str) -> str:
    """Теперь retry для ссылок — это вручную выполнить подготовку (без авто-ретраев)."""
    t = load_task(task_id)
    if t.get("url") and not t.get("video_path"):
        await process_task(task_id)  # ручная попытка
        return "ручной запуск подготовки выполнен"
    # для локальных медиа — перепланируем отправку
    now = datetime.now(TZ)
    run_dt = pick_run_datetime(now, RANDOM_DAYS_MAX)
    t["run_at"] = run_dt.isoformat()
    set_status(t, "SCHEDULED", None)
    schedule_send_at(task_id, run_dt)
    return f"перепланировал отправку на {run_dt.strftime('%Y-%m-%d %H:%M')}"

def _kb_error_row(task_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔁 Повторить", callback_data=f"e:retry:{task_id}"),
        InlineKeyboardButton(text="👁 Обзор",     callback_data=f"e:view:{task_id}"),
        InlineKeyboardButton(text="🗑 Удалить",   callback_data=f"e:del:{task_id}"),
    ]])
@dp.message(F.text == "/review_unprep")
async def review_unprep_cmd(msg: Message):
    items = _filter_tasks_for_review("unprep")
    if not items:
        await msg.reply("Неподготовленных ссылок нет ✅")
        return
    await _render_one_review(msg.chat.id, "unprep", items[0])

@dp.message(F.text == "/errors")
async def errors_cmd(msg: Message):
    errs = []
    for t in list_tasks():
        st = (t.get("status") or "").upper()
        if st in ("ERROR", "FAILED"):
            errs.append(t)
    if not errs:
        await msg.reply("Ошибок нет ✅")
        return
    def k(t):
        ra = t.get("next_retry_at") or t.get("created_at") or ""
        with suppress(Exception):
            return datetime.fromisoformat(ra)
        return datetime.now(TZ)
    errs.sort(key=k)
    for t in errs[:20]:
        tid = t["id"]
        src = t.get("url") or Path(t.get("video_path") or t.get("photo_path") or "").name or "-"
        err = (t.get("last_error") or "").strip()
        if len(err) > 350: err = err[:347] + "…"
        st = (t.get("status") or "").upper()
        text = (f"{'❌' if st=='ERROR' else '⛔'} {st}\n"
                f"id: {tid[:8]}\n"
                f"src: {src}\n"
                f"{('причина: ' + err) if err else ''}").strip()
        await msg.reply(text, reply_markup=_kb_error_row(tid), disable_web_page_preview=True)

@dp.callback_query(F.data.startswith("e:"))
async def errors_cb(cb):
    try:
        _, action, task_id = cb.data.split(":", 2)
    except Exception:
        await cb.answer("Некорректная команда", show_alert=True)
        return
    if action == "retry":
        try:
            note = await retry_task(task_id)
            await cb.answer("Ок")
            await bot.send_message(cb.message.chat.id, f"🔁 Задача {task_id[:8]}: {note}")
        except Exception as e:
            await cb.answer("Ошибка", show_alert=True)
            await bot.send_message(cb.message.chat.id, f"Не удалось перезапустить: {e}")
        return
    if action == "del":
        try:
            delete_task(task_id)
            await cb.answer("Удалено")
            await bot.send_message(cb.message.chat.id, f"🗑 Задача {task_id[:8]} удалена.")
        except Exception as e:
            await cb.answer("Ошибка", show_alert=True)
            await bot.send_message(cb.message.chat.id, f"Не удалось удалить: {e}")
        return
    if action == "view":
        try:
            t = load_task(task_id)
        except Exception:
            await cb.answer("Задача не найдена", show_alert=True); return
        mode = "links" if t.get("url") else "media"
        await _render_one_review(cb.message.chat.id, mode, t)
        await cb.answer()
        return
    await cb.answer("Неизвестная команда", show_alert=True)

# ===== Пауза/статус/next + next-кнопки =====

def next_window_start(ts: datetime) -> datetime:
    start = ts.replace(hour=POST_WINDOW_START, minute=0, second=0, microsecond=0)
    end   = ts.replace(hour=POST_WINDOW_END,   minute=0, second=0, microsecond=0)
    if ts < start:
        return start
    if ts >= end:
        nxt = ts + timedelta(days=1)
        return nxt.replace(hour=POST_WINDOW_START, minute=0, second=0, microsecond=0)
    return ts

def earliest_run_at() -> datetime | None:
    best = None
    for t in list_tasks():
        st = (t.get("status") or "").upper()
        if st not in ("READY", "SCHEDULED"):
            continue
        ra = t.get("run_at")
        if not ra:
            continue
        with suppress(Exception):
            dt = datetime.fromisoformat(ra)
            if (best is None) or (dt < best):
                best = dt
    return best

def _gap_delay_needed_at(ts: datetime) -> timedelta | None:
    if MIN_GAP_MINUTES <= 0:
        return None
    last = _read_last_sent_at()
    if not last:
        return None
    need = timedelta(minutes=MIN_GAP_MINUTES) - (ts - last)
    if need <= timedelta(0):
        return None
    return need

def next_task() -> tuple[dict | None, datetime | None]:
    best_t, best_dt = None, None
    for t in list_tasks():
        st = (t.get("status") or "").upper()
        if st not in ("READY", "SCHEDULED"):
            continue
        ra = t.get("run_at")
        if not ra:
            continue
        try:
            dt = datetime.fromisoformat(ra)
        except Exception:
            continue
        if (best_dt is None) or (dt < best_dt):
            best_t, best_dt = t, dt
    return best_t, best_dt

@dp.message(F.text == "/next")
async def next_cmd(msg: Message):
    now = datetime.now(TZ)

    t, run_at = next_task()
    if not t:
        await msg.reply("Очередь пуста — ближайшего поста нет.")
        return

    cand = run_at if run_at and run_at > now else now
    cand = next_window_start(cand)
    delay_gap = _gap_delay_needed_at(cand)
    jitter_note = ""
    if delay_gap:
        cand = cand + delay_gap
        jitter_note = f" + джиттер {max(0, GAP_JITTER_MIN_MIN)}–{max(GAP_JITTER_MIN_MIN, GAP_JITTER_MAX_MIN)} мин"
        cand = next_window_start(cand)

    src = t.get("url") or Path(t.get("video_path") or t.get("photo_path") or "").name or "-"
    pause_line = f"\n⏸ Пауза включена (с {pause_since_str()})." if is_paused() else ""
    eta = human_td(cand - now)
    txt = (
        f"Следующий пост:\n"
        f"• id: {t['id'][:8]}\n"
        f"• src: {src}\n"
        f"• запланировано: {cand:%Y-%m-%d %H:%M} ({eta}){jitter_note}{pause_line}\n\n"
        f"Отправить этот пост прямо сейчас?"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🚀 Отправить сейчас", callback_data=f"nx:send:{t['id']}"),
        InlineKeyboardButton(text="👁 Обзор",             callback_data=f"nx:view:{t['id']}")
    ]])
    await msg.reply(txt, reply_markup=kb, disable_web_page_preview=True)

@dp.callback_query(F.data.startswith("nx:"))
async def next_cb(cb):
    try:
        _, action, task_id = cb.data.split(":", 2)
    except Exception:
        await cb.answer("Некорректная команда", show_alert=True)
        return
    if action == "view":
        try:
            t = load_task(task_id)
        except Exception:
            await cb.answer("Задача не найдена", show_alert=True)
            return
        mode = "links" if t.get("url") else "media"
        await _render_one_review(cb.message.chat.id, mode, t)
        await cb.answer()
        return
    if action == "send":
        try:
            await cb.answer("Отправляю…")
            await send_task_force(task_id, target_chat_id=cb.message.chat.id)
            await bot.send_message(cb.message.chat.id, "✅ Отправлено.")
        except Exception as e:
            await bot.send_message(cb.message.chat.id, f"❌ Ошибка отправки: {e}")
        return
    await cb.answer("Ок")

# ===== Ручная команда подготовки =====

@dp.message(F.text.startswith("/process"))
async def process_cmd(msg: Message):
    parts = msg.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await msg.reply("Использование: /process <id> (ссылка или отложенное локальное видео)")
        return
    tid = parts[1].strip()
    t = None
    for x in list_tasks():
        if x["id"] == tid or x["id"].startswith(tid):
            t = x; break
    if not t:
        await msg.reply("Задача не найдена."); return
    try:
        note = await msg.reply("Готовлю…")
        await process_task(t["id"])
        await safe_edit(note, "🎬 Подготовка завершена. Переведено в READY.")
    except Exception as e:
        await msg.reply(f"❌ Ошибка подготовки: {e}")

@dp.message(F.text.startswith(("/prepare_all", "/prep_all")))
async def prepare_all_cmd(msg: Message):
    # опционально: /prepare_all 20  -> подготовить первые 20
    parts = msg.text.strip().split()
    limit = None
    if len(parts) > 1:
        with suppress(Exception):
            limit = max(1, int(parts[1]))

    # берём только неподготовленные ссылки (mode="unprep" уже реализован)
    items = _filter_tasks_for_review("unprep")
    if limit is not None:
        items = items[:limit]

    if not items:
        await msg.reply("Неподготовленных ссылок нет ✅")
        return

    note = await msg.reply(f"Стартую подготовку: {len(items)} шт…")
    ok = fail = 0
    progress_step = 5  # как часто обновлять статус сообщением

    for i, t in enumerate(items, start=1):
        try:
            await process_task(t["id"])  # та же ручная подготовка, что по кнопке 🎬
            ok += 1
        except Exception as e:
            fail += 1
            logger.exception("prepare_all: %s", e)
        if (i % progress_step == 0) or (i == len(items)):
            await safe_edit(
                note,
                f"Готовлю… {i}/{len(items)}\n"
                f"Успехов: {ok} | Ошибок: {fail}"
            )

    await msg.reply(f"Готово. Подготовлено: {ok}. Ошибок: {fail}.")

# ==================== Startup ====================

async def on_startup():
    logger.info("Scheduler started")
    scheduler.start()
    if OVERDUE_SCAN_MINUTES > 0:
        with suppress(Exception):
            scheduler.remove_job("scan_overdue")
        scheduler.add_job(
            send_overdue,
            trigger="interval",
            minutes=max(1, OVERDUE_SCAN_MINUTES),
            id="scan_overdue",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=300,
        )
        logger.info("Overdue scanner scheduled every %s min", OVERDUE_SCAN_MINUTES)
    if HISTORY_ENABLED:
        history_init()
    restore_on_start()
    await bot.set_my_commands([
        BotCommand(command="review_unprep", description="Просмотр неподготовленных ссылок"),
        BotCommand(command="prepare_all", description="Подготовить все неподготовленные ссылки"),
        BotCommand(command="review_id", description="Показать задачу по id"),
        BotCommand(command="start",        description="Что делает бот"),
        BotCommand(command="help",         description="Как пользоваться"),
        BotCommand(command="queue",        description="Очередь ссылок (живые)"),
        BotCommand(command="queue_media",  description="Очередь медиа (живые)"),
        BotCommand(command="queue_all",    description="Очередь — все задачи"),
        BotCommand(command="review",       description="Просмотр ссылок с кнопками"),
        BotCommand(command="review_media", description="Просмотр медиа с кнопками"),
        BotCommand(command="review_all",   description="Просмотр всех с кнопками"),
        BotCommand(command="errors",       description="Задачи с ошибками"),
        BotCommand(command="pause",        description="Остановить публикации"),
        BotCommand(command="resume",       description="Возобновить публикации"),
        BotCommand(command="status",       description="Состояние бота"),
        BotCommand(command="next",         description="Когда выйдет следующий пост"),
        BotCommand(command="process",      description="Ручная подготовка ссылки: /process <id>"),
    ])
    logger.info("Start polling")

@dp.message(F.text == "/help")
async def help_cmd(msg: Message):
    await msg.reply(
        "Команды:\n"
        "• /queue, /queue_media, /queue_all — показать очередь (ETA может быть отрицательным)\n"
        "• /review — обзор задач (🎬 подготовить для ссылок, 🚀 отправить сейчас)\n"
        "• /process <id> — вручную скачать/подготовить ссылку\n"
        "• /errors — задачи с ошибками (🔁 повтор — тоже ручной)\n"
        "• /pause /resume /status — управление паузой публикаций\n"
        "• /next — когда выйдет следующий пост ()\n"
        "Кинь ссылку (YT/VK), фото или видео — поставлю в очередь. Ссылки скачиваются только по твоей команде."
    )

# ==================== Entrypoint ====================

async def main():
    await on_startup()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
