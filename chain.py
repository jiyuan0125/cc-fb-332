#!/usr/bin/env python3
"""Import nightly inventory snapshots into chain.db and verify chain integrity.

Usage:
    python3 -I chain.py [snaps_dir] [chain_db] [integrity_txt]

Defaults (relative to this script):
    snaps_dir     = <script_dir>/snaps
    chain_db      = <script_dir>/out/chain.db
    integrity_txt = <script_dir>/out/integrity.txt

Files are processed in filename ascending order, which is the arrival order
(not the version order). Each snapshot file has three header lines
(version, prev, digest) followed by "sku,qty" body lines; a trailing "void"
line marks the snapshot as voided and is part of the digested body.
"""

import hashlib
import os
import sqlite3
import sys

ZERO_DIGEST = "0" * 64
CHECK_NAMES = ("version_seq", "digest_link", "digest_recalc",
               "void_rollback", "endpoints")


def read_snapshot(path):
    """Parse one snapshot file and recalculate its digest."""
    with open(path, "rb") as fh:
        raw = fh.read()
    text = raw.decode("utf-8")

    # Split the three header lines from the body; the body keeps its
    # final newline because the digest is computed over the full body text.
    cut = 0
    for _ in range(3):
        cut = text.index("\n", cut) + 1
    header = text[:cut].split("\n")
    version = int(header[0].split(",", 1)[1])
    prev = header[1].split(",", 1)[1]
    digest = header[2].split(",", 1)[1]
    body = text[cut:]

    body_lines = body.split("\n")
    if body_lines and body_lines[-1] == "":
        body_lines.pop()
    is_void = bool(body_lines) and body_lines[-1] == "void"
    if is_void:
        body_lines = body_lines[:-1]

    entries = []
    for line in body_lines:
        sku, qty = line.split(",", 1)
        entries.append((sku, int(qty)))

    recalc = hashlib.sha256(
        (prev + "\n" + body).encode("utf-8")
    ).hexdigest()

    return {
        "raw": raw,
        "version": version,
        "prev": prev,
        "digest": digest,
        "recalc": recalc,
        "void": is_void,
        "entries": entries,
    }


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    snaps_dir = sys.argv[1] if len(sys.argv) > 1 else os.path.join(script_dir, "snaps")
    db_path = sys.argv[2] if len(sys.argv) > 2 else os.path.join(script_dir, "out", "chain.db")
    integrity_path = sys.argv[3] if len(sys.argv) > 3 else os.path.join(script_dir, "out", "integrity.txt")

    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(integrity_path)), exist_ok=True)

    # Filename ascending == arrival order.
    filenames = sorted(
        name for name in os.listdir(snaps_dir)
        if os.path.isfile(os.path.join(snaps_dir, name))
    )

    records = {}
    for filename in filenames:
        records[filename] = read_snapshot(os.path.join(snaps_dir, filename))

    # Rebuild the database from scratch so it holds exactly the three
    # required tables even if an older schema lived at this path.
    if os.path.exists(db_path):
        os.remove(db_path)
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.executescript(
        """
        CREATE TABLE snapshots (
            version  INTEGER PRIMARY KEY,
            filename TEXT    NOT NULL UNIQUE,
            digest   TEXT    NOT NULL,
            status   TEXT    NOT NULL CHECK (status IN ('ok', 'void'))
        );
        CREATE TABLE rejects (
            filename TEXT    PRIMARY KEY,
            version  INTEGER NOT NULL,
            reason   TEXT    NOT NULL
        );
        CREATE TABLE deltas (
            from_ver INTEGER NOT NULL,
            to_ver   INTEGER NOT NULL,
            sku      TEXT    NOT NULL,
            delta    INTEGER NOT NULL,
            PRIMARY KEY (from_ver, to_ver, sku)
        );
        """
    )

    # ---- Import in arrival order --------------------------------------
    accepted_file = {}  # version -> filename of first accepted snapshot
    for filename in filenames:
        rec = records[filename]
        version = rec["version"]
        status = "void" if rec["void"] else "ok"

        if version in accepted_file:
            kept_file = accepted_file[version]
            if rec["raw"] == records[kept_file]["raw"]:
                # Same version, byte-identical content: ignore silently.
                continue
            # Same version, different content: reject, first arrival stays.
            cur.execute(
                "INSERT OR IGNORE INTO rejects (filename, version, reason) "
                "VALUES (?, ?, ?)",
                (filename, version, "version_conflict"),
            )
            continue

        cur.execute(
            "INSERT INTO snapshots (version, filename, digest, status) "
            "VALUES (?, ?, ?, ?)",
            (version, filename, rec["digest"], status),
        )
        accepted_file[version] = filename

    # ---- Deltas between adjacent effective (non-void) snapshots -------
    accepted = [
        records[accepted_file[v]]
        for v in sorted(accepted_file)
    ]
    effective = [r for r in accepted if not r["void"]]

    for before, after in zip(effective, effective[1:]):
        qty_before = dict(before["entries"])
        qty_after = dict(after["entries"])
        for sku in sorted(set(qty_before) | set(qty_after)):
            delta = qty_after.get(sku, 0) - qty_before.get(sku, 0)
            if delta != 0:
                cur.execute(
                    "INSERT INTO deltas (from_ver, to_ver, sku, delta) "
                    "VALUES (?, ?, ?, ?)",
                    (before["version"], after["version"], sku, delta),
                )

    # ---- Integrity checks ---------------------------------------------
    versions = [r["version"] for r in accepted]
    contig = versions == list(range(1, max(versions) + 1))
    results = {"version_seq": "ok" if contig else "broken"}

    # Every stored digest must reproduce from the raw header + body.
    recalc_ok = all(r["recalc"] == r["digest"] for r in accepted)
    results["digest_recalc"] = "ok" if recalc_ok else "broken"

    # Effective chain: each prev must equal the previous effective digest.
    link_break = None
    prior = None
    for rec in effective:
        if prior is not None and rec["prev"] != prior["digest"]:
            link_break = rec["version"]
            break
        prior = rec
    results["digest_link"] = "ok" if link_break is None else "broken"

    # Void snapshots are rolled back: the next effective snapshot links
    # straight to the last effective snapshot before the voided night.
    eff_by_ver = {r["version"]: r for r in effective}
    rollback_break = None
    for rec in accepted:
        if not rec["void"]:
            continue
        v = rec["version"]
        preds = [x for x in eff_by_ver if x < v]
        succs = [x for x in eff_by_ver if x > v]
        if not preds or not succs:
            rollback_break = v
            break
        predecessor = eff_by_ver[max(preds)]
        successor = eff_by_ver[min(succs)]
        if successor["prev"] != predecessor["digest"]:
            rollback_break = v
            break
    results["void_rollback"] = "ok" if rollback_break is None else "broken"

    # Endpoints: v1 anchored at the zero digest, head at the max version.
    first = accepted[0]
    last = accepted[-1]
    endpoints_ok = (
        first["version"] == 1
        and first["prev"] == ZERO_DIGEST
        and last["version"] == max(versions)
        and not last["void"]
    )
    results["endpoints"] = "ok" if endpoints_ok else "broken"

    verdict = "broken" if any(results[name] == "broken" for name in CHECK_NAMES) else "ok"
    break_at = link_break if link_break is not None else rollback_break

    lines = ["[checks]"]
    lines.extend(f"{name} {results[name]}" for name in CHECK_NAMES)
    lines.append("[chain]")
    lines.append(f"verdict {verdict}")
    if verdict == "broken":
        if break_at is not None:
            lines.append(f"break_at {break_at}")
        elif not contig:
            gap = next(v for v in range(1, max(versions) + 1) if v not in set(versions))
            lines.append(f"break_at {gap}")
    with open(integrity_path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("\n".join(lines) + "\n")

    con.commit()
    delta_rows = con.execute("SELECT COUNT(*) FROM deltas").fetchone()[0]
    rejected = con.execute("SELECT COUNT(*) FROM rejects").fetchone()[0]
    void_count = con.execute(
        "SELECT COUNT(*) FROM snapshots WHERE status='void'"
    ).fetchone()[0]
    con.close()

    print(
        f"imported={len(accepted)} rejected={rejected} void={void_count} "
        f"effective_pairs={len(effective) - 1} delta_rows={delta_rows} "
        f"verdict={verdict}"
        + (f" break_at={break_at}" if break_at is not None else "")
    )


if __name__ == "__main__":
    main()
