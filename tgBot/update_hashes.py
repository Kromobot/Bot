
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# update_hashes.py — добавляет content_hash ко всем старым задачам с медиа,
# чтобы работала дедупликация. Также переносит файлы в queue/media/, если нужно.
#
# Запуск (из папки проекта):
#     python update_hashes.py [QUEUE_DIR]
# По умолчанию QUEUE_DIR = "queue".

import sys
import json
import hashlib
import shutil
from pathlib import Path

QUEUE_DIR = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("queue")
TASKS_DIR = QUEUE_DIR / "tasks"
MEDIA_DIR = QUEUE_DIR / "media"
TASKS_DIR.mkdir(parents=True, exist_ok=True)
MEDIA_DIR.mkdir(parents=True, exist_ok=True)

def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _load(p: Path):
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def _save(p: Path, data: dict):
    tmp = p.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(p)

def _unique_path(target: Path) -> Path:
    if not target.exists():
        return target
    base, ext = target.stem, target.suffix
    i = 1
    while True:
        cand = target.with_name(f"{base}_{i}{ext}")
        if not cand.exists():
            return cand
        i += 1

def main():
    total = changed = hashed = moved = 0
    missing_files = 0
    hash_map = {}

    # Перенести json из корня в tasks (если вдруг остались)
    for p in QUEUE_DIR.glob("*.json"):
        dest = TASKS_DIR / p.name
        if dest.exists():
            p.unlink(missing_ok=True)
        else:
            p.replace(dest)

    for p in TASKS_DIR.glob("*.json"):
        try:
            t = _load(p)
        except Exception as e:
            print(f"[skip] {p.name}: bad json ({e})")
            continue
        total += 1
        t_id = t.get("id") or p.stem

        # определить путь к медиа
        media_key = None
        media_path = None
        for key in ("photo_path", "video_path"):
            val = t.get(key)
            if val:
                media_key = key
                media_path = Path(val)
                break

        if not media_path:
            if "content_hash" not in t:
                t["content_hash"] = None
                _save(p, t); changed += 1
            continue

        if not media_path.exists():
            missing_files += 1
            t["content_hash"] = None
            _save(p, t); changed += 1
            continue

        # Если файл вне MEDIA_DIR — переложим
        if MEDIA_DIR not in media_path.parents:
            new_path = _unique_path(MEDIA_DIR / media_path.name)
            shutil.move(str(media_path), str(new_path))
            t[media_key] = str(new_path)
            media_path = new_path
            moved += 1

        # Посчитать hash, если нет
        if not t.get("content_hash"):
            try:
                h = _sha256(media_path)
                t["content_hash"] = h
                _save(p, t)
                hashed += 1
            except Exception as e:
                print(f"[warn] can't hash {media_path}: {e}")
                continue
        else:
            h = t["content_hash"]

        if h:
            hash_map.setdefault(h, []).append(t_id)

    # Отчёт
    dups = {h: ids for h, ids in hash_map.items() if len(ids) > 1}
    print("==== update_hashes report ====")
    print(f"tasks total      : {total}")
    print(f"tasks changed    : {changed}")
    print(f"files moved      : {moved}")
    print(f"hashes computed  : {hashed}")
    print(f"missing files    : {missing_files}")
    print(f"duplicate groups : {len(dups)}")
    if dups:
        print("--- duplicates (hash -> task_ids) ---")
        for h, ids in dups.items():
            print(h, ":", ", ".join(ids))

if __name__ == "__main__":
    main()
