"""
OpenAlex Snapshot Extractor
===========================
Streams OpenAlex works data directly from S3 (no download needed),
filters to articles from 2015-2024, extracts relevant fields,
and writes to local Parquet part files.

Pausable/resumable: tracks progress in a JSON checkpoint file.
Press Ctrl+C to pause cleanly. Re-run to resume where you left off.

After extraction, run `python extract_works.py --merge` to combine
part files into a single Parquet file.

Usage:
    python extract_works.py                  # Start or resume extraction
    python extract_works.py --workers 8      # Use 8 parallel download threads
    python extract_works.py --status         # Show progress without running
    python extract_works.py --merge          # Combine part files into one
    python extract_works.py --reset          # Clear checkpoint and start over
"""

import argparse
import gzip
import json
import os
import signal
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from tqdm import tqdm

# Use orjson if available (2-3x faster JSON parsing, written in Rust)
try:
    from orjson import loads as json_loads
except ImportError:
    from json import loads as json_loads

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MANIFEST_URL = "https://openalex.s3.amazonaws.com/data/works/manifest"
S3_BASE = "https://openalex.s3.amazonaws.com/"

OUTPUT_DIR = Path(__file__).parent / "output"
PARTS_DIR = OUTPUT_DIR / "parts"
MERGED_PARQUET = OUTPUT_DIR / "works_extracted.parquet"
CHECKPOINT_FILE = OUTPUT_DIR / "checkpoint.json"
MANIFEST_CACHE = OUTPUT_DIR / "manifest.json"

# Filter criteria
MIN_YEAR = 2015
MAX_YEAR = 2024
WORK_TYPE = "article"

# How many records to buffer before flushing to a Parquet part file
FLUSH_EVERY = 100_000

# Streaming: read response in 1MB chunks
STREAM_CHUNK_SIZE = 1024 * 1024

# Retry config
MAX_RETRIES = 5
RETRY_BACKOFF = 10  # seconds, doubles each retry


# ---------------------------------------------------------------------------
# Arrow schema for output Parquet
# ---------------------------------------------------------------------------

PARQUET_SCHEMA = pa.schema([
    # Core identifiers & type info
    ("work_id", pa.string()),
    ("title", pa.string()),
    ("doi", pa.string()),
    ("type", pa.string()),                    # OpenAlex normalized type (always "article" given our filter)
    ("location_raw_type", pa.string()),        # primary_location.raw_type (e.g. "journal-article")
    ("is_paratext", pa.bool_()),              # Always False given our filter, stored for verification
    ("has_fulltext", pa.bool_()),
    ("ids_pmid", pa.string()),                 # PubMed ID (strong signal of real biomedical paper)
    ("publication_year", pa.int16()),
    ("publication_date", pa.string()),
    ("language", pa.string()),
    ("indexed_in", pa.list_(pa.string())),

    # Journal / source
    ("source_id", pa.string()),
    ("locations_count", pa.int16()),           # Number of locations (0 = suspicious)
    ("source_name", pa.string()),
    ("source_issn_l", pa.string()),
    ("source_type", pa.string()),
    ("source_version", pa.string()),          # primary_location.version (e.g. "publishedVersion")
    ("publisher", pa.string()),

    # Biblio (volume, issue, pages)
    ("biblio_volume", pa.string()),
    ("biblio_issue", pa.string()),
    ("biblio_first_page", pa.string()),
    ("biblio_last_page", pa.string()),

    # Citation metrics
    ("cited_by_count", pa.int32()),
    ("fwci", pa.float32()),
    ("citation_percentile", pa.float32()),
    ("is_top_1_pct", pa.bool_()),
    ("is_top_10_pct", pa.bool_()),

    # Open access
    ("is_oa", pa.bool_()),
    ("oa_status", pa.string()),
    ("apc_list_usd", pa.int32()),
    ("apc_paid_usd", pa.int32()),

    # Primary topic / field classification
    ("topic_field_id", pa.string()),
    ("topic_field_name", pa.string()),
    ("topic_subfield_id", pa.string()),
    ("topic_subfield_name", pa.string()),
    ("topic_domain_id", pa.string()),
    ("topic_domain_name", pa.string()),
    ("topic_score", pa.float32()),

    # All topics (not just primary)
    ("topic_ids", pa.list_(pa.string())),
    ("topic_names", pa.list_(pa.string())),
    ("topic_scores", pa.list_(pa.float32())),

    # Author info
    ("author_count", pa.int16()),
    ("first_author_name", pa.string()),
    ("last_author_name", pa.string()),
    ("countries_distinct_count", pa.int16()),
    ("institutions_distinct_count", pa.int16()),
    ("referenced_works_count", pa.int16()),
    ("is_retracted", pa.bool_()),

    # Institution data (all unique across all authors)
    ("institution_ids", pa.list_(pa.string())),
    ("institution_names", pa.list_(pa.string())),
    ("institution_types", pa.list_(pa.string())),
    ("institution_country_codes", pa.list_(pa.string())),
    ("institution_lineage_ids", pa.list_(pa.string())),  # Flattened lineage for tier matching

    # Corresponding author institutions
    ("corresponding_institution_ids", pa.list_(pa.string())),

    # First and last author institution IDs
    ("first_author_institution_ids", pa.list_(pa.string())),
    ("last_author_institution_ids", pa.list_(pa.string())),

])


# ---------------------------------------------------------------------------
# Extraction logic: JSON work -> flat row
# ---------------------------------------------------------------------------

def safe_get(d, *keys, default=None):
    """Safely navigate nested dicts."""
    for k in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(k)
        if d is None:
            return default
    return d


def extract_row(work):
    """Extract a flat row dict from a raw OpenAlex work JSON object."""
    pl = work.get("primary_location") or {}
    source = pl.get("source") or {}
    cnp = work.get("citation_normalized_percentile") or {}
    pt = work.get("primary_topic") or {}
    apc_list = work.get("apc_list") or {}
    apc_paid = work.get("apc_paid") or {}
    oa = work.get("open_access") or {}
    biblio = work.get("biblio") or {}

    # Authorships — extract author names, institution info, and lineage
    authorships = work.get("authorships") or []
    all_inst_ids = set()
    all_inst_names = set()
    all_inst_types = set()
    all_country_codes = set()
    all_lineage_ids = set()
    first_author_insts = []
    last_author_insts = []
    first_author_name = None
    last_author_name = None

    for auth in authorships:
        insts = auth.get("institutions") or []
        inst_ids = [i.get("id") for i in insts if i.get("id")]
        country_codes = [i.get("country_code") for i in insts if i.get("country_code")]
        all_inst_ids.update(inst_ids)
        all_country_codes.update(country_codes)

        for inst in insts:
            name = inst.get("display_name")
            if name:
                all_inst_names.add(name)
            itype = inst.get("type")
            if itype:
                all_inst_types.add(itype)
            lineage = inst.get("lineage") or []
            all_lineage_ids.update(lineage)

        pos = auth.get("author_position")
        if pos == "first":
            first_author_insts = inst_ids
            first_author_name = safe_get(auth, "author", "display_name")
        elif pos == "last":
            last_author_insts = inst_ids
            last_author_name = safe_get(auth, "author", "display_name")

    corr_insts = work.get("corresponding_institution_ids") or []

    # All topics (not just primary)
    all_topics = work.get("topics") or []
    topic_ids = [t.get("id") for t in all_topics if t.get("id")]
    topic_names = [t.get("display_name") for t in all_topics if t.get("display_name")]
    topic_scores = [t.get("score") for t in all_topics if t.get("score") is not None]

    return {
        "work_id": work.get("id"),
        "title": work.get("title"),
        "doi": work.get("doi"),
        "type": work.get("type"),
        "location_raw_type": pl.get("raw_type"),
        "is_paratext": work.get("is_paratext"),
        "has_fulltext": work.get("has_fulltext"),
        "ids_pmid": safe_get(work, "ids", "pmid"),
        "publication_year": work.get("publication_year"),
        "publication_date": work.get("publication_date"),
        "language": work.get("language"),
        "indexed_in": work.get("indexed_in") or [],

        "source_id": source.get("id"),
        "locations_count": work.get("locations_count"),
        "source_name": source.get("display_name"),
        "source_issn_l": source.get("issn_l"),
        "source_type": source.get("type"),
        "source_version": pl.get("version"),
        "publisher": safe_get(source, "host_organization_name"),

        "biblio_volume": biblio.get("volume"),
        "biblio_issue": biblio.get("issue"),
        "biblio_first_page": biblio.get("first_page"),
        "biblio_last_page": biblio.get("last_page"),

        "cited_by_count": work.get("cited_by_count"),
        "fwci": work.get("fwci"),
        "citation_percentile": cnp.get("value"),
        "is_top_1_pct": cnp.get("is_in_top_1_percent"),
        "is_top_10_pct": cnp.get("is_in_top_10_percent"),

        "is_oa": oa.get("is_oa"),
        "oa_status": oa.get("oa_status"),
        "apc_list_usd": apc_list.get("value_usd"),
        "apc_paid_usd": apc_paid.get("value_usd"),

        "topic_field_id": safe_get(pt, "field", "id"),
        "topic_field_name": safe_get(pt, "field", "display_name"),
        "topic_subfield_id": safe_get(pt, "subfield", "id"),
        "topic_subfield_name": safe_get(pt, "subfield", "display_name"),
        "topic_domain_id": safe_get(pt, "domain", "id"),
        "topic_domain_name": safe_get(pt, "domain", "display_name"),
        "topic_score": pt.get("score"),

        "topic_ids": topic_ids,
        "topic_names": topic_names,
        "topic_scores": topic_scores,

        "author_count": len(authorships),
        "first_author_name": first_author_name,
        "last_author_name": last_author_name,
        "countries_distinct_count": work.get("countries_distinct_count"),
        "institutions_distinct_count": work.get("institutions_distinct_count"),
        "referenced_works_count": work.get("referenced_works_count"),
        "is_retracted": work.get("is_retracted"),

        "institution_ids": sorted(all_inst_ids),
        "institution_names": sorted(all_inst_names),
        "institution_types": sorted(all_inst_types),
        "institution_country_codes": sorted(all_country_codes),
        "institution_lineage_ids": sorted(all_lineage_ids),
        "corresponding_institution_ids": corr_insts,
        "first_author_institution_ids": first_author_insts,
        "last_author_institution_ids": last_author_insts,
    }


# ---------------------------------------------------------------------------
# S3 streaming helpers
# ---------------------------------------------------------------------------

def fetch_manifest():
    """Download and cache the works manifest."""
    if MANIFEST_CACHE.exists():
        with open(MANIFEST_CACHE) as f:
            return json.load(f)

    print("Downloading manifest...")
    resp = requests.get(MANIFEST_URL, timeout=30)
    resp.raise_for_status()
    manifest = resp.json()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_CACHE, "w") as f:
        json.dump(manifest, f)

    return manifest


def stream_gz_lines(s3_url):
    """Stream a .gz file from S3, decompress on-the-fly, yield JSON lines.

    Uses streaming decompression so we never hold the full file in memory.
    """
    if s3_url.startswith("s3://openalex/"):
        http_url = S3_BASE + s3_url[len("s3://openalex/"):]
    else:
        http_url = s3_url

    retries = 0
    while True:
        try:
            resp = requests.get(http_url, stream=True, timeout=(15, 60))
            resp.raise_for_status()

            # Stream into a gzip decompressor
            decompressor = gzip.GzipFile(fileobj=resp.raw)
            # Wrap in a text reader for line-by-line iteration
            leftover = b""
            for chunk in iter(lambda: decompressor.read(STREAM_CHUNK_SIZE), b""):
                data = leftover + chunk
                lines = data.split(b"\n")
                # Last element may be incomplete
                leftover = lines.pop()
                for line in lines:
                    line = line.strip()
                    if line:
                        yield line  # yield bytes — json_loads accepts bytes

            # Handle final leftover
            if leftover.strip():
                yield leftover.strip()

            resp.close()
            return

        except (requests.RequestException, IOError, EOFError) as e:
            retries += 1
            if retries > MAX_RETRIES:
                raise RuntimeError(
                    f"Failed after {MAX_RETRIES} retries on {http_url}: {e}"
                )
            wait = RETRY_BACKOFF * (2 ** (retries - 1))
            print(f"\n  Retry {retries}/{MAX_RETRIES} for {http_url} "
                  f"(waiting {wait}s): {e}")
            time.sleep(wait)


# ---------------------------------------------------------------------------
# Checkpoint management
# ---------------------------------------------------------------------------

def load_checkpoint():
    """Load set of completed file URLs and part counter."""
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE) as f:
            data = json.load(f)
        return set(data.get("completed_files", [])), data.get("next_part", 0)
    return set(), 0


def save_checkpoint(completed_files, next_part):
    """Save set of completed file URLs and part counter."""
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({
            "completed_files": sorted(completed_files),
            "next_part": next_part,
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        }, f, indent=2)


# ---------------------------------------------------------------------------
# Parquet part-file writer
# ---------------------------------------------------------------------------

class ParquetPartWriter:
    """Buffers rows and writes them as numbered Parquet part files.

    Each part file is self-contained. This avoids the O(n^2) cost of
    reading + appending to a single growing file. After extraction,
    use --merge to combine all parts.

    Checkpoint integration: tracks which S3 files contributed to each
    batch so we only mark files as complete when their rows are safely
    on disk.
    """

    def __init__(self, parts_dir, schema, flush_every, start_part=0):
        self.parts_dir = Path(parts_dir)
        self.parts_dir.mkdir(parents=True, exist_ok=True)
        self.schema = schema
        self.flush_every = flush_every
        self.buffer = []
        self.total_written = 0
        self.next_part = start_part
        # S3 file URLs whose rows are in the current buffer (not yet flushed)
        self.pending_files = set()

    def add(self, row):
        self.buffer.append(row)
        if len(self.buffer) >= self.flush_every:
            return self.flush()
        return None

    def mark_file_done(self, url):
        """Mark an S3 file as having all its rows in the buffer."""
        self.pending_files.add(url)

    def flush(self):
        """Write buffer to a part file. Returns set of S3 files that are
        now safely persisted (can be checkpointed), or None if nothing to flush."""
        if not self.buffer:
            flushed_files = self.pending_files.copy()
            self.pending_files.clear()
            return flushed_files if flushed_files else None

        columns = {field.name: [] for field in self.schema}
        for row in self.buffer:
            for field in self.schema:
                columns[field.name].append(row.get(field.name))

        table = pa.table(columns, schema=self.schema)
        part_path = self.parts_dir / f"part_{self.next_part:05d}.parquet"
        pq.write_table(table, part_path, compression="zstd")

        self.total_written += len(self.buffer)
        self.next_part += 1
        self.buffer.clear()

        flushed_files = self.pending_files.copy()
        self.pending_files.clear()
        return flushed_files

    def close(self):
        """Flush remaining buffer. Returns set of final persisted files."""
        return self.flush()


# ---------------------------------------------------------------------------
# Main extraction
# ---------------------------------------------------------------------------

def count_parts_rows():
    """Count total rows across all part files."""
    total = 0
    total_bytes = 0
    if PARTS_DIR.exists():
        for f in sorted(PARTS_DIR.glob("part_*.parquet")):
            meta = pq.read_metadata(f)
            total += meta.num_rows
            total_bytes += f.stat().st_size
    return total, total_bytes


def show_status():
    """Show current extraction progress."""
    manifest = fetch_manifest()
    entries = manifest["entries"]
    total_files = len(entries)
    total_records = manifest["meta"]["record_count"]
    total_bytes = manifest["meta"]["content_length"]

    completed, next_part = load_checkpoint()
    completed_count = len(completed)

    completed_bytes = 0
    completed_records = 0
    for entry in entries:
        if entry["url"] in completed:
            completed_bytes += entry["meta"]["content_length"]
            completed_records += entry["meta"]["record_count"]

    remaining_bytes = total_bytes - completed_bytes

    print(f"\n{'='*60}")
    print(f"  OpenAlex Works Extraction Status")
    print(f"{'='*60}")
    print(f"  Files:   {completed_count:,} / {total_files:,} "
          f"({completed_count/total_files*100:.1f}%)")
    print(f"  Records: {completed_records:,} / {total_records:,} scanned")
    print(f"  Data:    {completed_bytes/1e9:.1f} GB / {total_bytes/1e9:.1f} GB "
          f"streamed")
    print(f"  Remaining: {remaining_bytes/1e9:.1f} GB")

    rows, part_bytes = count_parts_rows()
    if rows:
        print(f"\n  Output:  {rows:,} articles in {next_part} part files "
              f"({part_bytes/1e6:.0f} MB)")
    else:
        print(f"\n  Output:  No data extracted yet")

    if MERGED_PARQUET.exists():
        meta = pq.read_metadata(MERGED_PARQUET)
        size_mb = MERGED_PARQUET.stat().st_size / 1e6
        print(f"  Merged:  {meta.num_rows:,} rows ({size_mb:.0f} MB)")

    print(f"{'='*60}\n")


def merge_parts():
    """Combine all part files into a single Parquet file."""
    if not PARTS_DIR.exists():
        print("No part files found. Run extraction first.")
        return

    part_files = sorted(PARTS_DIR.glob("part_*.parquet"))
    if not part_files:
        print("No part files found. Run extraction first.")
        return

    print(f"Merging {len(part_files)} part files...")
    tables = []
    for f in tqdm(part_files, desc="Reading parts"):
        tables.append(pq.read_table(f, schema=PARQUET_SCHEMA))

    combined = pa.concat_tables(tables)
    print(f"Writing {combined.num_rows:,} rows to {MERGED_PARQUET}...")
    pq.write_table(combined, MERGED_PARQUET, compression="zstd")

    size_mb = MERGED_PARQUET.stat().st_size / 1e6
    print(f"Done! {MERGED_PARQUET} ({size_mb:.0f} MB)")


def process_one_file(s3_url):
    """Download and process a single S3 file. Returns (url, rows, records_scanned).

    This runs in a thread pool for parallel downloading.
    """
    rows = []
    scanned = 0
    for line in stream_gz_lines(s3_url):
        try:
            work = json_loads(line)
        except (ValueError, TypeError):
            continue

        scanned += 1
        pub_year = work.get("publication_year")
        work_type = work.get("type")
        is_paratext = work.get("is_paratext", False)

        if (pub_year is not None
                and MIN_YEAR <= pub_year <= MAX_YEAR
                and work_type == WORK_TYPE
                and not is_paratext):
            rows.append(extract_row(work))

    return s3_url, rows, scanned


def run_extraction(workers=4):
    """Main extraction loop with parallel S3 downloads."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    manifest = fetch_manifest()
    entries = manifest["entries"]
    total_files = len(entries)
    total_bytes = manifest["meta"]["content_length"]

    completed, next_part = load_checkpoint()
    print(f"\nManifest: {total_files:,} files, "
          f"{manifest['meta']['record_count']:,} total works, "
          f"{total_bytes/1e9:.1f} GB compressed")

    if completed:
        print(f"Resuming: {len(completed):,} files already done, "
              f"next part: {next_part}")

    # Sort entries by size (smallest first) so we see progress quickly
    remaining = [e for e in entries if e["url"] not in completed]
    remaining.sort(key=lambda e: e["meta"]["content_length"])

    if not remaining:
        print("All files already processed!")
        show_status()
        return

    remaining_bytes = sum(e["meta"]["content_length"] for e in remaining)
    print(f"Remaining: {len(remaining):,} files, "
          f"{remaining_bytes/1e9:.1f} GB")
    print(f"Workers: {workers}\n")

    # Build a lookup: url -> file_bytes
    entry_bytes = {e["url"]: e["meta"]["content_length"] for e in remaining}

    # Graceful shutdown on Ctrl+C
    shutdown_requested = False

    def signal_handler(sig, frame):
        nonlocal shutdown_requested
        if shutdown_requested:
            print("\nForce quit.")
            sys.exit(1)
        shutdown_requested = True
        print("\n\nShutting down workers after in-flight files... "
              "(Ctrl+C again to force quit)")

    signal.signal(signal.SIGINT, signal_handler)

    writer = ParquetPartWriter(
        PARTS_DIR, PARQUET_SCHEMA, FLUSH_EVERY, start_part=next_part
    )
    articles_found = 0
    records_scanned = 0
    files_done_this_session = 0

    progress = tqdm(
        total=remaining_bytes,
        unit="B",
        unit_scale=True,
        desc="Streaming from S3",
        bar_format=("{l_bar}{bar}| {n_fmt}/{total_fmt} "
                    "[{elapsed}<{remaining}, {rate_fmt}]"),
    )

    def worker_init():
        """Make child processes ignore SIGINT — only the main process handles it."""
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    try:
        with ProcessPoolExecutor(max_workers=workers, initializer=worker_init) as pool:
            # Submit initial batch
            futures = {}
            idx = 0

            def submit_next():
                nonlocal idx
                while idx < len(remaining) and len(futures) < workers:
                    entry = remaining[idx]
                    f = pool.submit(process_one_file, entry["url"])
                    futures[f] = entry["url"]
                    idx += 1

            submit_next()

            while futures and not shutdown_requested:
                # Collect completed futures (non-blocking check)
                done = [f for f in futures if f.done()]
                if not done:
                    time.sleep(0.1)
                    continue

                for f in done:
                    url = futures.pop(f)
                    try:
                        _, rows, scanned = f.result()
                    except Exception as e:
                        print(f"\n  ERROR processing {url}: {e}")
                        progress.update(entry_bytes.get(url, 0))
                        continue

                    records_scanned += scanned
                    articles_found += len(rows)

                    for row in rows:
                        flushed = writer.add(row)
                        if flushed:
                            completed.update(flushed)
                            save_checkpoint(completed, writer.next_part)

                    writer.mark_file_done(url)
                    files_done_this_session += 1
                    progress.update(entry_bytes.get(url, 0))
                    progress.set_postfix(
                        articles=f"{articles_found:,}",
                        scanned=f"{records_scanned:,}",
                        files=f"{len(completed) + len(writer.pending_files):,}"
                              f"/{total_files:,}",
                    )

                submit_next()

            # If shutting down, wait for in-flight futures to finish
            if shutdown_requested and futures:
                print("  Waiting for in-flight downloads to finish...")
                for f in list(futures):
                    url = futures.pop(f)
                    try:
                        _, rows, scanned = f.result(timeout=120)
                        records_scanned += scanned
                        articles_found += len(rows)
                        for row in rows:
                            writer.add(row)
                        writer.mark_file_done(url)
                        files_done_this_session += 1
                        progress.update(entry_bytes.get(url, 0))
                    except Exception:
                        pass  # Will retry on next run

    finally:
        # Flush remaining buffer and checkpoint
        final_files = writer.close()
        if final_files:
            completed.update(final_files)
        save_checkpoint(completed, writer.next_part)
        progress.close()

        print(f"\n{'='*60}")
        print(f"  Session Summary")
        print(f"{'='*60}")
        print(f"  Files processed this session: {files_done_this_session:,}")
        print(f"  Records scanned:  {records_scanned:,}")
        print(f"  Articles extracted: {articles_found:,}")
        print(f"  Total files done: {len(completed):,} / {total_files:,}")

        rows, part_bytes = count_parts_rows()
        if rows:
            print(f"  Part files: {writer.next_part} files, "
                  f"{rows:,} rows ({part_bytes/1e6:.0f} MB)")

        if shutdown_requested:
            print(f"\n  Paused. Run again to resume.")
        elif len(completed) == total_files:
            print(f"\n  DONE! Run with --merge to combine part files.")
        print(f"{'='*60}\n")


def reset():
    """Clear all output to start over."""
    if CHECKPOINT_FILE.exists():
        os.remove(CHECKPOINT_FILE)
        print("Checkpoint cleared.")
    if MERGED_PARQUET.exists():
        os.remove(MERGED_PARQUET)
        print("Merged file removed.")
    if PARTS_DIR.exists():
        for f in PARTS_DIR.glob("part_*.parquet"):
            os.remove(f)
        PARTS_DIR.rmdir()
        print("Part files removed.")
    if MANIFEST_CACHE.exists():
        os.remove(MANIFEST_CACHE)
        print("Manifest cache cleared.")
    print("Ready for fresh start.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Extract OpenAlex works from S3 snapshot to Parquet"
    )
    parser.add_argument(
        "--status", action="store_true",
        help="Show extraction progress",
    )
    parser.add_argument(
        "--merge", action="store_true",
        help="Combine part files into single Parquet",
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Clear checkpoint and start over",
    )
    parser.add_argument(
        "--workers", type=int, default=4,
        help="Number of parallel download threads (default: 4)",
    )
    args = parser.parse_args()

    if args.reset:
        reset()
    elif args.status:
        show_status()
    elif args.merge:
        merge_parts()
    else:
        run_extraction(workers=args.workers)


if __name__ == "__main__":
    main()
