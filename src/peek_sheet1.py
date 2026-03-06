import re
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

XLSX = Path("data/raw/TCH_activity_report_2026-02-15_03-13-30.xlsx")
SHEET_XML = "xl/worksheets/sheet1.xml"
SHARED_XML = "xl/sharedStrings.xml"

N_ROWS = 3  # preview rows after header

def read_row1_pairs(z: zipfile.ZipFile) -> list[tuple[str,int]]:
    # extract row 1 snippet (already proven in /tmp, but we do it direct for portability)
    s = z.read(SHEET_XML).decode("utf-8", errors="ignore")
    m = re.search(r'<row r="1"[^>]*>(.*?)</row>', s, re.DOTALL)
    if not m:
        raise RuntimeError("Row 1 not found")
    row = m.group(1)
    pairs = re.findall(r'<c r="([A-Z]+)1"[^>]*t="s"[^>]*>.*?<v>(\d+)</v>.*?</c>', row, re.DOTALL)
    return [(col, int(idx)) for col, idx in pairs]

def iter_shared_strings(z: zipfile.ZipFile):
    ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with z.open(SHARED_XML) as f:
        # iterparse needs a file-like object
        for event, elem in ET.iterparse(f, events=("end",)):
            if elem.tag.endswith("}si"):
                txt = "".join((t.text or "") for t in elem.findall(".//m:t", ns)).strip()
                yield txt
                elem.clear()

def build_headers(z: zipfile.ZipFile) -> list[str]:
    pairs = read_row1_pairs(z)
    max_idx = max(idx for _, idx in pairs)
    vals = []
    for s in iter_shared_strings(z):
        vals.append(s)
        if len(vals) > max_idx:
            break
    # order in pairs is left-to-right
    headers = [vals[idx] for _, idx in pairs]
    return headers

def iter_rows(z: zipfile.ZipFile, start_row: int = 2, max_rows: int = 10):
    # stream parse sheet1 rows; we only need a few for peek
    ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with z.open(SHEET_XML) as f:
        context = ET.iterparse(f, events=("end",))
        for event, elem in context:
            if elem.tag.endswith("}row"):
                r = int(elem.attrib.get("r", "0"))
                if r < start_row:
                    elem.clear()
                    continue
                if r >= start_row + max_rows:
                    break

                # Build dict col_letter -> value_text (shared string index or number)
                row_vals = {}
                for c in elem.findall(".//m:c", ns):
                    cell_ref = c.attrib.get("r", "")
                    # column letters: A, B, ..., EC
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

def col_letters(n: int):
    # 1-indexed to letters
    letters = []
    for i in range(1, n+1):
        x = i
        s = ""
        while x:
            x, rem = divmod(x-1, 26)
            s = chr(65+rem) + s
        letters.append(s)
    return letters

def main():
    with zipfile.ZipFile(XLSX) as z:
        headers = build_headers(z)
        cols = col_letters(len(headers))
        col_to_name = dict(zip(cols, headers))

        print("Columns:", len(headers))
        print("First 10 headers:", headers[:10])

        # Build a tiny sharedStrings cache for indexes we see in the peek rows
        # We'll resolve shared-string cells on demand
        # For speed we just read the first ~2000 shared strings into a list (covers many headers + common values)
        shared_cache = []
        for i, s in enumerate(iter_shared_strings(z)):
            shared_cache.append(s)
            if i >= 20000:
                break

        def resolve(cell_type, text):
            if text is None:
                return None
            if cell_type == "s":
                idx = int(text)
                return shared_cache[idx] if idx < len(shared_cache) else f"<s:{idx}>"
            return text

        print("\nPreview rows:")
        for r, row_vals in iter_rows(z, start_row=2, max_rows=N_ROWS):
            out = {}
            for col, (t, txt) in row_vals.items():
                name = col_to_name.get(col, col)
                out[name] = resolve(t, txt)
            # print only first 12 keys for readability
            keys = list(out.keys())[:12]
            short = {k: out[k] for k in keys}
            print(f"row {r}:", short)

if __name__ == "__main__":
    main()
