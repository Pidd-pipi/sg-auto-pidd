#!/usr/bin/env python3
"""Bump the canonical application version and static asset cache keys."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP_DIR))

from api.version import VERSION_FILE, bump_version, read_version  # noqa: E402


def update_static_asset_versions(root: Path, old: str, new: str, *, dry_run: bool = False) -> list[Path]:
    """Keep the ?v= cache key in every static HTML page aligned with VERSION."""
    changed: list[Path] = []
    for path in sorted((root / "static").glob("*.html")):
        text = path.read_text(encoding="utf-8")
        updated = text.replace(f"?v={old}", f"?v={new}")
        if updated == text:
            continue
        changed.append(path)
        if not dry_run:
            path.write_text(updated, encoding="utf-8")
    return changed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="升级 sologsb 调度台版本")
    parser.add_argument("part", metavar="major|minor|patch|X.Y.Z", help="升级级别或完整版本号")
    parser.add_argument("--dry-run", action="store_true", help="只显示结果，不修改文件")
    args = parser.parse_args(argv)

    old = read_version()
    try:
        new = bump_version(old, args.part)
    except ValueError as exc:
        parser.error(str(exc))
    if new == old:
        print(f"版本未变化：{old}")
        return 0

    print(f"版本：{old} → {new}")
    if args.dry_run:
        print("静态资源缓存键：dry-run，不修改")
        return 0

    VERSION_FILE.write_text(f"{new}\n", encoding="utf-8")
    changed = update_static_asset_versions(APP_DIR, old, new)
    print(f"已更新 {VERSION_FILE}")
    for path in changed:
        print(f"已更新 {path}")
    if not changed:
        print("静态页面未找到旧版本缓存键，请检查是否已手工调整")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
