"""
Parse the Briant et al. detailed result pages into CSVs.

The published .htm files are Excel "save as web page" exports. The top-level
file is only a frameset; the tables live in sibling sheet files, one per Excel
tab, with the tab names in tabstrip.htm. This follows the frame targets found
in the HTML rather than guessing directory names, and prints what it finds at
every step so a failure is visible instead of silent.

Usage:
    python parse_briant_results.py
"""

import io
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests

BASE = "https://pagesperso.g-scop.grenoble-inp.fr/~cambazah/sequencing/data"
PAGES = {
    "small": f"{BASE}/smallDataSetDetailedResults.htm",
    "large": f"{BASE}/largeDataSetDetailedResults.htm",
}
OUT_DIR = Path("./briant_results")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})


def get(url: str) -> str | None:
    """Fetch a URL, returning None on any failure. Never raises."""
    try:
        resp = SESSION.get(url, timeout=60)
    except requests.RequestException as exc:
        print(f"    [error] {url}: {exc}")
        return None
    if resp.status_code != 200:
        print(f"    [skip] {url}: HTTP {resp.status_code}")
        return None
    # Excel exports are usually cp1252; fall back to the detected encoding.
    resp.encoding = resp.apparent_encoding or "cp1252"
    return resp.text


def find_links(html: str, base_url: str, pattern: str) -> list[str]:
    """Absolute URLs from src= or href= attributes matching a pattern."""
    raw = re.findall(r'(?:src|href)\s*=\s*["\']([^"\']+)["\']', html, re.I)
    hits = [urljoin(base_url, r) for r in raw if re.search(pattern, r, re.I)]
    seen, out = set(), []
    for h in hits:
        if h not in seen:
            seen.add(h)
            out.append(h)
    return out


def discover_sheets(page_url: str) -> list[tuple[str, str]]:
    """Return (tab_name, sheet_url) for every tab, following the frameset."""
    print(f"  fetching frameset: {page_url}")
    html = get(page_url)
    if html is None:
        return []

    # The frameset points at the sheet files and at tabstrip.htm.
    frames = find_links(html, page_url, r"\.html?$")
    print(f"  frame targets: {[f.rsplit('/', 1)[-1] for f in frames]}")

    tabstrips = [f for f in frames if "tabstrip" in f.lower()]
    sheets = [f for f in frames if re.search(r"sheet\d+\.html?$", f, re.I)]

    # tabstrip.htm lists every sheet with its tab name.
    names: list[str] = []
    if tabstrips:
        print(f"  fetching tabstrip: {tabstrips[0]}")
        tab_html = get(tabstrips[0])
        if tab_html:
            pairs = re.findall(
                r'href\s*=\s*["\']([^"\']*sheet\d+\.html?)["\'][^>]*>(?:<[^>]+>)*([^<]+)',
                tab_html,
                re.I,
            )
            for href, label in pairs:
                url = urljoin(tabstrips[0], href)
                if url not in sheets:
                    sheets.append(url)
                names.append(label.strip())
            if not pairs:
                for href in find_links(tab_html, tabstrips[0], r"sheet\d+\.html?$"):
                    if href not in sheets:
                        sheets.append(href)

    if not sheets:
        print("  [warn] no sheet files found in frameset or tabstrip")
        return []

    sheets.sort()
    print(f"  sheets: {[s.rsplit('/', 1)[-1] for s in sheets]}")
    print(f"  tab names: {names or '(none found, using file names)'}")

    return [
        (names[i].strip() if i < len(names) else s.rsplit("/", 1)[-1].replace(".htm", ""), s)
        for i, s in enumerate(sheets)
    ]


def read_sheet(url: str) -> pd.DataFrame | None:
    html = get(url)
    if html is None:
        return None
    try:
        tables = pd.read_html(io.StringIO(html), decimal=",", thousands=None)
    except ValueError as exc:
        print(f"    [warn] no table in {url}: {exc}")
        return None
    if not tables:
        return None
    return max(tables, key=lambda t: t.shape[1])


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all").dropna(axis=1, how="all").copy()
    if df.empty:
        return df

    # Excel exports often carry the real header as the first data row.
    first = df.iloc[0].astype(str)
    if first.str.contains("Instance", case=False, na=False).any():
        df.columns = first
        df = df.iloc[1:]

    df.columns = [str(c).strip() for c in df.columns]
    df = df.reset_index(drop=True)

    for col in df.columns:
        if df[col].dtype == object:
            converted = pd.to_numeric(
                df[col].astype(str).str.replace(",", ".", regex=False),
                errors="coerce",
            )
            if converted.notna().sum() >= 0.8 * max(df[col].notna().sum(), 1):
                df[col] = converted
    return df


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    written = 0

    for page_label, page_url in PAGES.items():
        print(f"\n=== {page_label} ===")
        found = discover_sheets(page_url)
        if not found:
            print(f"  [fail] nothing discovered for {page_url}")
            continue

        for tab_name, url in found:
            df = read_sheet(url)
            if df is None or df.empty:
                print(f"  [skip] empty sheet: {url}")
                continue
            df = clean(df)

            safe = re.sub(r"[^A-Za-z0-9]+", "_", tab_name).strip("_") or "sheet"
            path = OUT_DIR / f"{page_label}_{safe}.csv"
            df.to_csv(path, index=False)
            written += 1

            print(f"\n  wrote {path}  rows={len(df)}")
            print(f"  columns: {list(df.columns)}")

    if written == 0:
        print("\nNo sheets parsed. Save the .htm pages locally from the browser "
              "and rerun with local paths, or open them in Excel and export xlsx.")
        sys.exit(1)
    print(f"\nDone. {written} sheet(s) written to {OUT_DIR}/")


if __name__ == "__main__":
    main()