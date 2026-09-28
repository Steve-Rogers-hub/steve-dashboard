"""
compare_scans.py - A/B the scan output before and after the signal-engine change.

Usage:
    python compare_scans.py baseline_2026-09-08.csv new_2026-09-08.csv

Both runs must cover the same bars. Run the NEW scan with --allow-partial-week
so the only difference between the two files is the signal logic itself.

Writes scan_diff.csv with one row per ticker whose scoring changed.
"""
import sys
import pandas as pd

pd.set_option("display.width", 200)
pd.set_option("display.max_rows", 100)

KEY = "ticker"
NUMERIC = ["scan_tii", "price_score", "volume_score", "ma_score", "macd_score"]
FLAGS = ["scan_qualifies", "entry_signal", "new_high", "trend_reversal"]


def as_bool(s):
    return s.astype(str).str.strip().str.lower().isin(["true", "1", "yes"])


def load(path):
    df = pd.read_csv(path)
    if KEY not in df.columns:
        sys.exit(f"{path}: no '{KEY}' column - is this a scan output CSV?")
    for c in NUMERIC:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in FLAGS:
        if c in df.columns:
            df[c] = as_bool(df[c])
    return df.set_index(KEY)


def section(title):
    print("\n" + "=" * 68)
    print(f"  {title}")
    print("=" * 68)


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    old, new = load(sys.argv[1]), load(sys.argv[2])

    section("COVERAGE")
    only_old = sorted(set(old.index) - set(new.index))
    only_new = sorted(set(new.index) - set(old.index))
    print(f"  baseline rows: {len(old)}    new rows: {len(new)}")
    if only_old:
        print(f"  dropped in new run ({len(only_old)}): {', '.join(only_old[:25])}")
    if only_new:
        print(f"  appeared in new run ({len(only_new)}): {', '.join(only_new[:25])}")

    # Errors are the first thing to check - a wave of them means data trouble,
    # not a logic change.
    if "scan_error" in old.columns and "scan_error" in new.columns:
        def err_count(df):
            e = df["scan_error"].fillna("").astype(str).str.strip()
            return e.ne("").sum()
        oe, ne = err_count(old), err_count(new)
        print(f"  tickers with scan_error:  baseline {oe}  ->  new {ne}")
        if ne > oe * 2 and ne > 20:
            print("  ** error count jumped sharply - check throttling/history "
                  "before reading anything else below **")

    both = old.index.intersection(new.index)
    o, n = old.loc[both], new.loc[both]

    section("PRICE SCORE - the component that was changed")
    dist = pd.DataFrame({
        "baseline": o["price_score"].value_counts().sort_index(),
        "new": n["price_score"].value_counts().sort_index(),
    }).fillna(0).astype(int)
    dist["change"] = dist["new"] - dist["baseline"]
    print(dist.to_string())

    moved = (o["price_score"] != n["price_score"]).sum()
    print(f"\n  price_score changed on {moved} of {len(both)} tickers "
          f"({100 * moved / len(both):.1f}%)")
    flipped = ((o["price_score"] == -3) & (n["price_score"] == 3)).sum()
    print(f"  flipped -3 -> +3: {flipped}   "
          f"(these were scored 'Trend Down' while trending up)")

    section("TII DISTRIBUTION")
    tii = pd.DataFrame({
        "baseline": o["scan_tii"].value_counts().sort_index(),
        "new": n["scan_tii"].value_counts().sort_index(),
    }).fillna(0).astype(int)
    tii["change"] = tii["new"] - tii["baseline"]
    print(tii.to_string())

    section("SIGNALS")
    for f in FLAGS:
        if f in o.columns and f in n.columns:
            gained = (~o[f] & n[f]).sum()
            lost = (o[f] & ~n[f]).sum()
            print(f"  {f:<16} baseline {int(o[f].sum()):>4}  ->  new {int(n[f].sum()):>4}"
                  f"   (+{gained} / -{lost})")

    section("NEW TREND REVERSALS - EYEBALL THESE ON A CHART")
    print("  The reversal branch went from near-unreachable to reachable.")
    print("  If the new logic is too loose, it shows up here first.\n")
    gained_rev = both[(~o["trend_reversal"]) & (n["trend_reversal"])]
    if len(gained_rev) == 0:
        print("  none")
    else:
        cols = [c for c in ["scan_tii", "price_score", "macd_score",
                            "scan_qualifies", "scan_notes"] if c in n.columns]
        print(n.loc[gained_rev, cols].sort_values("scan_tii", ascending=False)
              .head(30).to_string())
        print(f"\n  {len(gained_rev)} total. Pull up the top 3-5 weekly charts and "
              f"confirm the pattern is actually there:")
        print("  a low, a rally high, a HIGHER low, then a close back through that high.")

    section("QUALIFIERS")
    gq = both[(~o["scan_qualifies"]) & (n["scan_qualifies"])]
    lq = both[(o["scan_qualifies"]) & (~n["scan_qualifies"])]
    print(f"  newly qualifying ({len(gq)}): {', '.join(sorted(gq)[:40]) or 'none'}")
    print(f"  no longer qualifying ({len(lq)}): {', '.join(sorted(lq)[:40]) or 'none'}")

    section("WRITING scan_diff.csv")
    changed = both[(o["price_score"] != n["price_score"]) |
                   (o["scan_tii"] != n["scan_tii"]) |
                   (o["scan_qualifies"] != n["scan_qualifies"])]
    out = pd.DataFrame(index=changed)
    for c in ["price_score", "scan_tii", "trend_reversal", "new_high", "scan_qualifies"]:
        if c in o.columns:
            out[f"old_{c}"] = o.loc[changed, c]
            out[f"new_{c}"] = n.loc[changed, c]
    if "scan_notes" in n.columns:
        out["new_notes"] = n.loc[changed, "scan_notes"]
    out.sort_values("new_scan_tii", ascending=False).to_csv("scan_diff.csv")
    print(f"  {len(out)} changed tickers written to scan_diff.csv")


if __name__ == "__main__":
    main()
