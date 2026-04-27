"""One-time ingestion script: parse the GHG Protocol Scope 3 Standard PDF and build a ChromaDB index.

Run with:
    python -m rag.ingest
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import chromadb
import pdfplumber
from sentence_transformers import SentenceTransformer

PDF_PATH = Path(__file__).parent.parent / "data" / "Corporate-Value-Chain-Accounting-Reporing-Standard_041613_2.pdf"
RAG_DB_PATH = Path(__file__).parent / "ghg_index"
COLLECTION_NAME = "ghg_scope3_standard"
EMBEDDING_MODEL = "all-MiniLM-L6-v2"

# ── Regex patterns ────────────────────────────────────────────────────────────

_CHAPTER_HEADER_RE = re.compile(r"CHAPTER\s+(\d{1,2})\s+(.+)")

# Section headers: chapter 1–15 only, title must start with uppercase letter.
# This prevents body-text numbers like "94.5 t CO2e" from matching.
_SECTION_RE = re.compile(r"^([1-9]|1[0-5])\.(\d{1,2})\s+([A-Z].+)")

_APPENDIX_RE = re.compile(r"^Appendix\s+([A-Z])\.\s+(.+)")

# Category descriptions: "Category N: Name"
# Handles optional space from PDF ligature breaks (e.g. "F uel-")
_CATEGORY_RE = re.compile(r"Category\s+(\d{1,2})\s*:\s*(.+)")

# Used to split lines containing two merged category headers
_CATEGORY_SPLIT_RE = re.compile(r"(?=\bCategory\s+\d{1,2}\s*:)")

# ── Topic tag keyword map ─────────────────────────────────────────────────────

_TOPIC_KEYWORDS: dict[str, list[str]] = {
    "category_1": ["category 1", "purchased goods", "purchased services"],
    "category_2": ["category 2", "capital goods"],
    "category_3": ["category 3", "fuel- and energy"],
    "category_4": ["category 4", "upstream transportation"],
    "category_5": ["category 5", "waste generated"],
    "category_6": ["category 6", "business travel"],
    "category_7": ["category 7", "employee commuting"],
    "category_8": ["category 8", "upstream leased"],
    "category_9": ["category 9", "downstream transportation"],
    "category_10": ["category 10", "processing of sold products"],
    "category_11": ["category 11", "use of sold products"],
    "category_12": ["category 12", "end-of-life"],
    "category_13": ["category 13", "downstream leased"],
    "category_14": ["category 14", "franchises"],
    "category_15": ["category 15", "investments"],
    "boundary": ["boundary", "scope 3 boundary", "value chain", "operational boundaries"],
    "data_quality": ["data quality", "primary data", "secondary data", "supplier data", "activity data"],
    "allocation": ["allocation", "allocate emissions", "economic allocation", "physical allocation"],
    "reporting": ["reporting", "publicly report", "disclosure", "required information"],
    "assurance": ["assurance", "verification", "third-party", "assured"],
    "reduction_target": ["reduction target", "ghg reduction", "base year", "track emissions"],
    "calculation_methods": ["spend-based", "average-data", "hybrid method", "supplier-specific", "emission factor"],
}


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class Chunk:
    chunk_id: str
    text: str
    chapter_num: int
    chapter_title: str
    section_num: str
    section_title: str
    start_page: int
    end_page: int
    category_num: int = 0        # 0 = not a per-category sub-chunk
    category_name: str = ""
    topic_tags: list[str] = field(default_factory=list)

    @property
    def source_citation(self) -> str:
        base = f"GHG Protocol Scope 3 Standard, Chapter {self.chapter_num} ({self.chapter_title})"
        if self.section_num:
            base += f", Section {self.section_num} ({self.section_title})"
        if self.category_num:
            base += f", Category {self.category_num} ({self.category_name})"
        base += f", p.{self.start_page}"
        return base


# ── Helpers ───────────────────────────────────────────────────────────────────

def _assign_topic_tags(text: str) -> list[str]:
    lower = text.lower()
    return [tag for tag, kws in _TOPIC_KEYWORDS.items() if any(kw in lower for kw in kws)]


# Canonical names for the 15 Scope 3 categories — used as ground truth when
# PDF extraction produces bleed artifacts in the category header line.
_CANONICAL_CATEGORY_NAMES: dict[int, str] = {
    1: "Purchased goods and services",
    2: "Capital goods",
    3: "Fuel- and energy-related activities",
    4: "Upstream transportation and distribution",
    5: "Waste generated in operations",
    6: "Business travel",
    7: "Employee commuting",
    8: "Upstream leased assets",
    9: "Downstream transportation and distribution",
    10: "Processing of sold products",
    11: "Use of sold products",
    12: "End-of-life treatment of sold products",
    13: "Downstream leased assets",
    14: "Franchises",
    15: "Investments",
}


def _normalize_category_name(raw: str) -> str:
    """Fix PDF ligature breaks and strip two-column bleed from category names.

    Handles: 'F uel-' → 'Fuel-', 'U pstream' → 'Upstream'.
    Truncates at first body-text bleed signal (lowercase word after the name,
    Box reference, or parenthetical that belongs to the right column).
    """
    # Fix broken ligatures: single uppercase letter + space + lowercase
    fixed = re.sub(r"\b([A-Z])\s+([a-z])", r"\1\2", raw)
    # Strip trailing right-column bleed starting at common bleed patterns
    fixed = re.sub(r"\s+(activities in the|not included|proportional|Box\s*\[).*", "", fixed, flags=re.IGNORECASE)
    # Strip trailing Box/Figure references
    fixed = re.sub(r"\s+Box\s*\[.*", "", fixed)
    return fixed.strip()


def _split_merged_section_headers(line: str) -> list[str]:
    """Split a line containing two merged N.N section headers."""
    matches = list(_SECTION_RE.finditer(line))
    if len(matches) < 2:
        return [line]
    split_pos = matches[1].start()
    return [line[:split_pos].strip(), line[split_pos:].strip()]


def _split_merged_category_headers(line: str) -> list[str]:
    """Split a line containing two merged 'Category N:' headers."""
    parts = _CATEGORY_SPLIT_RE.split(line)
    return [p.strip() for p in parts if p.strip()]


# ── Parsing ───────────────────────────────────────────────────────────────────

def _extract_pages(pdf_path: Path) -> list[tuple[int, str]]:
    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            pages.append((i + 1, text))
    return pages


def _parse_chunks(pages: list[tuple[int, str]]) -> list[Chunk]:  # noqa: C901
    chunks: list[Chunk] = []
    used_ids: set[str] = set()

    # Section-level state
    cur_chap_num = 0
    cur_chap_title = "Front Matter"
    cur_sec_num = ""
    cur_sec_title = "Introduction"
    cur_sec_start = 1

    # Category-level state (active when cur_cat_num > 0)
    cur_cat_num = 0
    cur_cat_name = ""
    cur_cat_start = 1

    cur_lines: list[str] = []

    def _make_id(base: str) -> str:
        uid = base
        n = 2
        while uid in used_ids:
            uid = f"{base}_{n}"
            n += 1
        used_ids.add(uid)
        return uid

    def flush(end_page: int) -> None:
        text = "\n".join(cur_lines).strip()
        if not text:
            return

        if cur_cat_num:
            cid = _make_id(f"ch{cur_chap_num}_s{cur_sec_num.replace('.', '_')}_cat{cur_cat_num}")
            chunk = Chunk(
                chunk_id=cid,
                text=text,
                chapter_num=cur_chap_num,
                chapter_title=cur_chap_title,
                section_num=cur_sec_num,
                section_title=cur_sec_title,
                start_page=cur_cat_start,
                end_page=end_page,
                category_num=cur_cat_num,
                category_name=cur_cat_name,
                topic_tags=_assign_topic_tags(text),
            )
        else:
            cid = _make_id(f"ch{cur_chap_num}_s{cur_sec_num.replace('.', '_') or '0'}")
            chunk = Chunk(
                chunk_id=cid,
                text=text,
                chapter_num=cur_chap_num,
                chapter_title=cur_chap_title,
                section_num=cur_sec_num,
                section_title=cur_sec_title,
                start_page=cur_sec_start,
                end_page=end_page,
                topic_tags=_assign_topic_tags(text),
            )

        chunks.append(chunk)

    def start_section(chap_num, chap_title, sec_num, sec_title, page):
        nonlocal cur_chap_num, cur_chap_title, cur_sec_num, cur_sec_title
        nonlocal cur_sec_start, cur_cat_num, cur_cat_name, cur_cat_start, cur_lines
        flush(page)
        cur_chap_num, cur_chap_title = chap_num, chap_title
        cur_sec_num, cur_sec_title = sec_num, sec_title
        cur_sec_start = page
        cur_cat_num, cur_cat_name, cur_cat_start = 0, "", page
        cur_lines = []

    def start_category(cat_num, cat_name, page, first_line):
        nonlocal cur_cat_num, cur_cat_name, cur_cat_start, cur_lines
        flush(page)
        cur_cat_num = cat_num
        cur_cat_name = cat_name
        cur_cat_start = page
        cur_lines = [first_line]

    for page_num, page_text in pages:
        for raw_line in page_text.split("\n"):
            line = raw_line.strip()
            if not line:
                cur_lines.append("")
                continue

            # ── Running chapter page header ───────────────────────────────
            ch_m = _CHAPTER_HEADER_RE.search(line)
            if ch_m:
                new_chap = int(ch_m.group(1))
                new_title = ch_m.group(2).strip()
                if new_chap != cur_chap_num:
                    start_section(new_chap, new_title, "", new_title, page_num)
                continue  # never add running headers to chunk text

            # ── Appendix header ───────────────────────────────────────────
            app_m = _APPENDIX_RE.match(line)
            if app_m:
                start_section(
                    cur_chap_num, cur_chap_title,
                    f"App.{app_m.group(1)}", app_m.group(2).strip(),
                    page_num,
                )
                cur_lines = [line]
                continue

            # ── Section header (N.N Title) ────────────────────────────────
            # First split in case two headers are merged on one line
            sub_lines = _split_merged_section_headers(line)
            handled_as_section = False
            for sub_line in sub_lines:
                sec_m = _SECTION_RE.match(sub_line)
                if sec_m:
                    handled_as_section = True
                    start_section(
                        cur_chap_num, cur_chap_title,
                        f"{sec_m.group(1)}.{sec_m.group(2)}",
                        sec_m.group(3).strip(),
                        page_num,
                    )
                    cur_lines = [sub_line]
                else:
                    # May still contain a category header — handled below
                    _process_body_line(sub_line, page_num, start_category)
            if handled_as_section:
                continue

            # ── Body line (may contain Category N: header) ────────────────
            _process_body_line(line, page_num, start_category)

    if pages:
        flush(pages[-1][0])

    return chunks


def _process_body_line(line: str, page_num: int, start_category_fn) -> None:
    """Detect Category N: headers in a body line and delegate to start_category_fn.

    Handles merged category headers on a single line by splitting first.
    Appends non-header text to the module-level cur_lines via the closure
    inside start_category_fn's enclosing scope — handled by passing cur_lines
    as a side-effect via the nonlocal in the outer function.
    """
    # This function is intentionally a thin router; state mutation happens
    # inside _parse_chunks via the closures start_section / start_category.
    # We need access to cur_lines here, so we use a workaround: return
    # instructions to the caller. But since Python closures don't easily
    # compose this way, we restructure: _process_body_line is inlined below.
    raise NotImplementedError  # replaced by inline logic — see _parse_chunks


# ── Inline body-line processing (replaces _process_body_line) ────────────────

# We patch _parse_chunks to inline the body-line logic directly.
# The function above is a placeholder replaced by the implementation below.

def _parse_chunks(pages: list[tuple[int, str]]) -> list[Chunk]:  # noqa: F811, C901
    chunks: list[Chunk] = []
    used_ids: set[str] = set()

    cur_chap_num = 0
    cur_chap_title = "Front Matter"
    cur_sec_num = ""
    cur_sec_title = "Introduction"
    cur_sec_start = 1
    cur_cat_num = 0
    cur_cat_name = ""
    cur_cat_start = 1
    cur_lines: list[str] = []

    def _make_id(base: str) -> str:
        uid = base
        n = 2
        while uid in used_ids:
            uid = f"{base}_{n}"
            n += 1
        used_ids.add(uid)
        return uid

    def flush(end_page: int) -> None:
        nonlocal cur_lines
        text = "\n".join(cur_lines).strip()
        if not text:
            cur_lines = []
            return
        if cur_cat_num:
            cid = _make_id(f"ch{cur_chap_num}_s{cur_sec_num.replace('.','_')}_cat{cur_cat_num}")
            chunk = Chunk(
                chunk_id=cid, text=text,
                chapter_num=cur_chap_num, chapter_title=cur_chap_title,
                section_num=cur_sec_num, section_title=cur_sec_title,
                start_page=cur_cat_start, end_page=end_page,
                category_num=cur_cat_num, category_name=cur_cat_name,
                topic_tags=_assign_topic_tags(text),
            )
        else:
            cid = _make_id(f"ch{cur_chap_num}_s{cur_sec_num.replace('.','_') or '0'}")
            chunk = Chunk(
                chunk_id=cid, text=text,
                chapter_num=cur_chap_num, chapter_title=cur_chap_title,
                section_num=cur_sec_num, section_title=cur_sec_title,
                start_page=cur_sec_start, end_page=end_page,
                topic_tags=_assign_topic_tags(text),
            )
        chunks.append(chunk)
        cur_lines = []

    def new_section(chap_num, chap_title, sec_num, sec_title, page):
        nonlocal cur_chap_num, cur_chap_title, cur_sec_num, cur_sec_title
        nonlocal cur_sec_start, cur_cat_num, cur_cat_name, cur_cat_start
        flush(page)
        cur_chap_num, cur_chap_title = chap_num, chap_title
        cur_sec_num, cur_sec_title = sec_num, sec_title
        cur_sec_start = page
        cur_cat_num, cur_cat_name, cur_cat_start = 0, "", page

    def new_category(cat_num, cat_name, page, header_line):
        nonlocal cur_cat_num, cur_cat_name, cur_cat_start
        flush(page)
        cur_cat_num = cat_num
        cur_cat_name = cat_name
        cur_cat_start = page
        cur_lines.append(header_line)

    for page_num, page_text in pages:
        for raw_line in page_text.split("\n"):
            line = raw_line.strip()
            if not line:
                cur_lines.append("")
                continue

            # Running chapter page header
            ch_m = _CHAPTER_HEADER_RE.search(line)
            if ch_m:
                new_chap = int(ch_m.group(1))
                new_title = ch_m.group(2).strip()
                if new_chap != cur_chap_num:
                    new_section(new_chap, new_title, "", new_title, page_num)
                continue

            # Appendix header
            app_m = _APPENDIX_RE.match(line)
            if app_m:
                new_section(
                    cur_chap_num, cur_chap_title,
                    f"App.{app_m.group(1)}", app_m.group(2).strip(), page_num,
                )
                cur_lines.append(line)
                continue

            # Possibly merged section headers — split and process each part
            sec_parts = _split_merged_section_headers(line)
            for part in sec_parts:
                sec_m = _SECTION_RE.match(part)
                if sec_m:
                    new_section(
                        cur_chap_num, cur_chap_title,
                        f"{sec_m.group(1)}.{sec_m.group(2)}",
                        sec_m.group(3).strip(), page_num,
                    )
                    cur_lines.append(part)
                    continue

                # Possibly merged category headers — split and process each part
                cat_parts = _split_merged_category_headers(part)
                for cat_part in cat_parts:
                    cat_m = _CATEGORY_RE.match(cat_part)
                    if cat_m:
                        cat_num = int(cat_m.group(1))
                        name = _CANONICAL_CATEGORY_NAMES.get(
                            cat_num, _normalize_category_name(cat_m.group(2))
                        )
                        new_category(cat_num, name, page_num, cat_part)
                    else:
                        cur_lines.append(cat_part)

    if pages:
        flush(pages[-1][0])

    return chunks


# ── Ingestion entry point ─────────────────────────────────────────────────────

def ingest(pdf_path: Path = PDF_PATH, db_path: Path = RAG_DB_PATH) -> None:
    print(f"Loading PDF: {pdf_path}")
    pages = _extract_pages(pdf_path)
    print(f"  {len(pages)} pages extracted")

    print("Parsing sections into chunks…")
    chunks = _parse_chunks(pages)
    print(f"  {len(chunks)} chunks created")

    # Print category sub-chunk summary
    cat_chunks = [c for c in chunks if c.category_num]
    if cat_chunks:
        print(f"  ↳ {len(cat_chunks)} per-category sub-chunks")
        for c in cat_chunks:
            print(f"    cat{c.category_num}: {c.category_name!r} | {c.section_num} | pp.{c.start_page}-{c.end_page}")

    print(f"Loading embedding model ({EMBEDDING_MODEL})…")
    model = SentenceTransformer(EMBEDDING_MODEL)

    texts = [c.text for c in chunks]
    print("Embedding chunks…")
    embeddings = model.encode(texts, show_progress_bar=True, batch_size=32)

    print(f"Writing to ChromaDB at {db_path}…")
    client = chromadb.PersistentClient(path=str(db_path))
    try:
        client.delete_collection(COLLECTION_NAME)
    except Exception:
        pass
    collection = client.create_collection(COLLECTION_NAME)

    collection.add(
        ids=[c.chunk_id for c in chunks],
        documents=texts,
        embeddings=[e.tolist() for e in embeddings],
        metadatas=[
            {
                "chapter_num": c.chapter_num,
                "chapter_title": c.chapter_title,
                "section_num": c.section_num,
                "section_title": c.section_title,
                "start_page": c.start_page,
                "end_page": c.end_page,
                "category_num": c.category_num,
                "category_name": c.category_name,
                "topic_tags": "|".join(c.topic_tags),
                "source_citation": c.source_citation,
            }
            for c in chunks
        ],
    )

    print(f"✅ Index built successfully — {len(chunks)} chunks in '{COLLECTION_NAME}'")


if __name__ == "__main__":
    ingest()
