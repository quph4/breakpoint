"""Generate human-readable reasons for a bet from the feature row.

Features are stored as A−B differences; we normalize by negating when the
model picks player B so every snippet reads as "in favor of the pick".
We only emit positive (supportive) snippets — empty list if the model
picked despite the headline features being neutral.
"""
from __future__ import annotations


def make_rationale(features: dict, pick_is_a: bool, surface: str | None) -> list[str]:
    """Return up to 4 short snippets explaining why this bet was placed."""
    sign = 1 if pick_is_a else -1
    surf = surface or "surface"

    def f(key: str) -> float | None:
        v = features.get(key)
        if v is None:
            return None
        try:
            return float(v) * sign
        except (TypeError, ValueError):
            return None

    snippets: list[tuple[float, str]] = []

    elo_surf = f("elo_surf_diff")
    if elo_surf is not None and elo_surf >= 30:
        snippets.append((abs(elo_surf) / 25, f"{surf} Elo +{int(elo_surf)}"))

    elo_o = f("elo_diff")
    if elo_o is not None and elo_o >= 50:
        snippets.append((abs(elo_o) / 50, f"Overall Elo +{int(elo_o)}"))

    f10 = f("form10_diff")
    if f10 is not None and f10 >= 0.2:
        snippets.append((abs(f10) / 0.1, f"Form +{f10 * 100:.0f}% last 10"))

    sfs = f("surf_form_diff")
    if sfs is not None and sfs >= 0.2:
        snippets.append((abs(sfs) / 0.1, f"{surf} form +{sfs * 100:.0f}%"))

    h2h = f("h2h_diff")
    if h2h is not None and h2h >= 0.4:
        snippets.append((abs(h2h) / 0.2, f"H2H tilts to pick"))

    sp = f("serve_pts_won_diff")
    if sp is not None and sp >= 0.03:
        snippets.append((abs(sp) / 0.02, f"Service pts +{sp * 100:.1f}%"))

    rp = f("return_pts_won_diff")
    if rp is not None and rp >= 0.03:
        snippets.append((abs(rp) / 0.02, f"Return pts +{rp * 100:.1f}%"))

    ssp = f("surf_serve_diff")
    if ssp is not None and ssp >= 0.03:
        snippets.append((abs(ssp) / 0.02, f"{surf} serve +{ssp * 100:.1f}%"))

    bps = f("bp_save_pct_diff")
    if bps is not None and bps >= 0.05:
        snippets.append((abs(bps) / 0.04, f"BP save +{bps * 100:.0f}%"))

    rt = f("rank_traj_diff")
    if rt is not None and abs(rt) >= 15:
        direction = "rising" if rt > 0 else "falling"
        snippets.append((abs(rt) / 15, f"Trending {direction} ({int(abs(rt))} ranks)"))

    h = f("height_diff_cm")
    if h is not None and h >= 6:
        snippets.append((abs(h) / 5, f"Taller by {int(h)}cm"))

    snippets.sort(reverse=True)
    return [s for _, s in snippets[:4]]
