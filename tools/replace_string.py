#!/usr/bin/env python3
import sys, pathlib

OLD = "tap_twitterapi"
NEW = "tap_twitterapi"
# edit the whitelist if needed
TEXT_EXT = {".py",".toml",".yml",".yaml",".md",".rst",".txt",".ini",".cfg",".json",".sh",".cfg",".lock",".pxd",".pyx"}

def is_text(p: pathlib.Path) -> bool:
    return p.suffix.lower() in TEXT_EXT

def replace_in(p: pathlib.Path):
    try:
        data = p.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        try:
            data = p.read_text(encoding="latin-1")
        except Exception:
            return
    if OLD in data:
        p.write_text(data.replace(OLD, NEW), encoding="utf-8")
        print(f"updated: {p}")

def main():
    root = pathlib.Path(".")
    for p in root.rglob("*"):
        if p.is_file() and not any(part.startswith(".git") for part in p.parts) and is_text(p):
            replace_in(p)

if __name__ == "__main__":
    main()
