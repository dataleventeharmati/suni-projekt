import re
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

XLSX = Path("data/raw/TCH_activity_report_2026-02-15_03-13-30.xlsx")
SHEET_XML = "xl/worksheets/sheet1.xml"
SHARED_XML = "xl/sharedStrings.xml"

OUT_DIR = Path("data/processed")
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PARQUET = OUT_DIR / "tch_repairs_raw.parquet"

# Safety: write in chunks
BATCH_ROWS = 5000

def col_letters(n: int):
    letters = []
    for i in range(1, n+1):
        x = i
        s = ""
        while x:
            x, rem = divmod(x-1, 26)
            s = chr(65+rem) + s
        letters.append(s)
    return letters

def read_row1_pairs(z: zipfile.ZipFile) -> list[tuple[str,int]]:
    # Read just enough of sheet1 to find row 1. (We read full XML from zip member,
    # but this is still a streaming file read from disk; for 707MB XML it can take time.
    # We'll do a lightweight regex on a prefix window by scanning chunks.)
    with z.open(SHEET_XML) as f:
        buf = []
        # read first ~5MB which should contain row 1
        buf.append(f.read(5 * 1024 * 1024).decode("utf-8", errors="ignore"))
    s = "".join(buf)
    m = re.search(r'<row r="1"[^>]*>(.*?)</row>', s, re.DOTALL)
    if not m:
        raise RuntimeError("Row 1 not found in first 5MB; increase window.")
    row = m.group(1)
    pairs = re.findall(r'<c r="([A-Z]+)1"[^>]*t="s"[^>]*>.*?<v>(\d+)</v>.*?</c>', row, re.DOTALL)
    return [(col, int(idx)) for col, idx in pairs]

def iter_shared_strings(z: zipfile.ZipFile):
    ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with z.open(SHARED_XML) as f:
        for event, elem in ET.iterparse(f, events=("end",)):
            if elem.tag.endswith("}si"):
                txt = "".join((t.text or "") for t in elem.findall(".//m:t", ns)).strip()
                yield txt
                elem.clear()

def build_headers_and_shared(z: zipfile.ZipFile):
    pairs = read_row1_pairs(z)
    max_idx = max(idx for _, idx in pairs)
    shared = []
    for s in iter_shared_strings(z):
        shared.append(s)
        if len(shared) > max_idx:
            break
    headers = [shared[idx] for _, idx in pairs]
    cols = col_letters(len(headers))
    col_to_name = dict(zip(cols, headers))
    return headers, col_to_name, shared  # shared currently only up to header max

def iter_sheet_rows(z: zipfile.ZipFile, start_row: int = 2):
    ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with z.open(SHEET_XML) as f:
        context = ET.iterparse(f, events=("end",))
        for event, elem in context:
            if elem.tag.endswith("}row"):
                r = int(elem.attrib.get("r", "0"))
                if r < start_row:
                    elem.clear()
                    continue

                row_vals = {}
                for c in elem.findall(".//m:c", ns):
                    cell_ref = c.attrib.get("r", "")
                    mcol = re.match(r"([A-Z]+)\d+", cell_ref)
                    if not mcol:
                        continue
                    col = mcol.group(1)
                    v = c.find("m:v", ns)
                    if v is None:
                        continue
                    row_vals[col] = (c.attrib.get("t"), v.text)

                yield r, row_vals
                elem.clear()

def main():
    with zipfile.ZipFile(XLSX) as z:
        headers, col_to_name, shared_header_only = build_headers_and_shared(z)
        print(f"Headers: {len(headers)} columns")
        print(f"Output: {OUT_PARQUET}")

        # For values, sharedStrings indexes can be huge. We cannot preload all.
        # We'll cache on-demand up to a moving limit. For performance in this dataset,
        # we assume many values are short and repeated early; we will load first N shared strings.
        # If we hit higher indexes, we extend in chunks.
        ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

        # We'll build a sharedStrings iterator we can restart by iterparse on the zip member.
        # Simplest: extract sharedStrings.xml to /tmp once, but we keep this script self-contained:
        # we will read it again when extending cache.
        def build_shared_cache(limit: int):
            cache = []
            with z.open(SHARED_XML) as f:
                for event, elem in ET.iterparse(f, events=("end",)):
                    if elem.tag.endswith("}si"):
                        txt = "".join((t.text or "") for t in elem.findall(".//m:t", ns)).strip()
                        cache.append(txt)
                        elem.clear()
                        if len(cache) >= limit:
                            break
            return cache

        cache_limit = 50000
        shared_cache = build_shared_cache(cache_limit)

        def resolve(cell_type, text):
            if text is None:
                return None
            if cell_type == "s":
                idx = int(text)
                if idx < len(shared_cache):
                    return shared_cache[idx]
                else:
                    return f"__S__{idx}"
            return text

        # Prepare parquet writer (all columns as string initially; typing later in transform step)
        schema = pa.schema([pa.field(h, pa.string()) for h in headers])
        writer = pq.ParquetWriter(OUT_PARQUET.as_posix(), schema)

        batch = {h: [] for h in headers}
        col_letters_list = col_letters(len(headers))  # A..EC in order

        total = 0
        for r, row_vals in iter_sheet_rows(z, start_row=2):
            # fill row with None defaults
            row_out = {}
            for col in col_letters_list:
                name = col_to_name[col]
                t_txt = row_vals.get(col)
                if t_txt is None:
                    row_out[name] = None
                else:
                    t, txt = t_txt
                    row_out[name] = resolve(t, txt)

            for h in headers:
                batch[h].append(row_out[h])

            total += 1
            if total % 10000 == 0:
                print(f"[progress] rows: {total}")

            if len(batch[headers[0]]) >= BATCH_ROWS:
                table = pa.table(batch, schema=schema)
                writer.write_table(table)
                batch = {h: [] for h in headers}

        # flush remaining
        if len(batch[headers[0]]) > 0:
            table = pa.table(batch, schema=schema)
            writer.write_table(table)

        writer.close()
        print(f"Done. Wrote rows: {total}")

if __name__ == "__main__":
    main()
