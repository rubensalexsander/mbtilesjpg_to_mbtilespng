import sqlite3
from io import BytesIO
from PIL import Image
import sys

def is_jpeg(blob: bytes) -> bool:
    return len(blob) >= 3 and blob[:3] == b"\xff\xd8\xff"

def jpg_to_png(blob: bytes) -> bytes:
    img = Image.open(BytesIO(blob))
    if img.mode not in ("RGB", "RGBA"):
        img = img.convert("RGB")
    out = BytesIO()
    img.save(out, format="PNG", optimize=True)
    return out.getvalue()

def main():
    if len(sys.argv) != 3:
        print("Usage: python script.py input.mbtiles output.mbtiles")
        sys.exit(1)

    src_path, dst_path = sys.argv[1], sys.argv[2]

    src = sqlite3.connect(src_path)
    dst = sqlite3.connect(dst_path)

    # Copy DB
    src.backup(dst)
    src.close()

    cur = dst.cursor()

    # Check tables
    tables = {r[0] for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}

    if not {"map", "images"}.issubset(tables):
        print("ERROR: This MBTiles does not use map/images schema.")
        sys.exit(2)

    total = cur.execute("SELECT COUNT(*) FROM images").fetchone()[0]
    print(f"Images total: {total}")

    batch = 2000
    offset = 0
    processed = 0
    converted = 0

    while True:
        rows = cur.execute(
            "SELECT tile_id, tile_data FROM images LIMIT ? OFFSET ?",
            (batch, offset)
        ).fetchall()

        if not rows:
            break

        dst.execute("BEGIN;")
        for tile_id, blob in rows:
            processed += 1

            if is_jpeg(blob):
                png = jpg_to_png(blob)
                cur.execute(
                    "UPDATE images SET tile_data=? WHERE tile_id=?",
                    (png, tile_id)
                )
                converted += 1
        dst.execute("COMMIT;")

        offset += batch

        percent = (processed / total) * 100
        print(
            f"Progress: {percent:6.2f}% "
            f"| processed={processed}/{total} "
            f"| converted={converted}"
        )

    # Update metadata
    cur.execute("DELETE FROM metadata WHERE name='format'")
    cur.execute("INSERT INTO metadata(name, value) VALUES('format', 'png')")
    dst.commit()

    dst.close()
    print("Done ✔  JPEG → PNG conversion finished")

if __name__ == "__main__":
    main()
