"""Bot Ara's Model 🧪 — scan GU / EU / DXY dans le cloud, message Telegram.

Modèle d'Antoine (2026-10-26) : anticiper un CRT H4 avant sa clôture.

    C0 = bloc H4 qui vient de fermer (grille NY : ouvertures 1/5/9/13/17/21 h)
    C1 = bloc H4 suivant, découpé en 4 H1 : Q1 Q2 Q3 Q4
    Q1 prend le high (ou le low) de C0
    Q2 fait turtle soup de Q1 : mèche au-delà de l'extrême de Q1, clôture
       revenue en deçà
    → entrée à l'open de Q3 (pari : C1 fermera en CRT, retour dans C0)

Tourne sur GitHub Actions (`.github/workflows/ara.yml`), indépendant du Mac.
Un setup n'est signalé que pendant Q3 (entre la clôture de Q2 et celle de Q3) :
après, l'entrée est passée. Le cron tire plusieurs fois par heure de Q3 pour
encaisser les retards / sauts du planificateur GitHub ; `state/seen.json`
(conservé entre les runs par actions/cache) évite le doublon.

Même règle que `~/tv-700s-mac/pine/ara_model.pine` et
`~/tv-700s-mac/src/tv_700s_mac/watcher/ara_alert.py` : les garder alignés.

Usage :
    python ara_bot.py                 # scan + Telegram (le run du cron)
    python ara_bot.py --dry-run       # affiche, n'envoie rien
    python ara_bot.py --history 30    # liste les setups des 30 derniers jours
    python ara_bot.py --test          # message de test sur Telegram
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import dukascopy_python
import pandas as pd
from dukascopy_python.instruments import (
    INSTRUMENT_FX_MAJORS_EUR_USD,
    INSTRUMENT_FX_MAJORS_GBP_USD,
    INSTRUMENT_IDX_AMERICA_DOLLAR_IDX_USD,
)

NY_TZ = ZoneInfo("America/New_York")
H1 = timedelta(hours=1)

SYMBOLS = {
    "GBPUSD": INSTRUMENT_FX_MAJORS_GBP_USD,
    "EURUSD": INSTRUMENT_FX_MAJORS_EUR_USD,
    "DXY": INSTRUMENT_IDX_AMERICA_DOLLAR_IDX_USD,
}

STATE_PATH = Path(__file__).resolve().parent / "state" / "seen.json"
FETCH_RETRIES = 3


def log(msg: str) -> None:
    print(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}Z {msg}", flush=True)


# ──────────────────────────────────────────────────────────────────
# Données
# ──────────────────────────────────────────────────────────────────

def fetch_h1(instrument, end: datetime, days: int) -> pd.DataFrame:
    """H1 Dukascopy (bid), index UTC, colonnes Open/High/Low/Close."""
    start = end - timedelta(days=days)
    last_err: Exception | None = None
    for attempt in range(1, FETCH_RETRIES + 1):
        try:
            df = dukascopy_python.fetch(instrument, dukascopy_python.INTERVAL_HOUR_1,
                                        dukascopy_python.OFFER_SIDE_BID, start, end,
                                        max_retries=1)
            if df is None or df.empty:
                raise ValueError("série vide")
            break
        except Exception as e:
            last_err = e
            log(f"fetch KO (essai {attempt}/{FETCH_RETRIES}) : {str(e)[:160]}")
            time.sleep(3 * attempt)
    else:
        raise RuntimeError(f"Dukascopy indisponible : {last_err}")

    if not isinstance(df.index, pd.DatetimeIndex):
        if "timestamp" in df.columns:
            df = df.set_index("timestamp")
        df.index = pd.to_datetime(df.index)
    df.index = df.index.tz_localize("UTC") if df.index.tz is None else df.index.tz_convert("UTC")
    df = df.rename(columns={c: str(c).capitalize() for c in df.columns})
    return df[["Open", "High", "Low", "Close"]].sort_index()


# ──────────────────────────────────────────────────────────────────
# Détection
# ──────────────────────────────────────────────────────────────────

def find_setups(h1: pd.DataFrame, now: datetime) -> list[dict]:
    """Tous les Ara's Model de la série, du plus ancien au plus récent.

    Bougie par bougie, comme l'indicateur Pine : le bloc H4 d'une H1 est son
    heure NY ramenée à la dernière ouverture 1/5/9/13/17/21 h. C0 = le bloc
    précédent présent dans la série (vendredi 13h–17h pour le bloc du dimanche
    soir). Les bougies pas encore closes à `now` sont ignorées.
    """
    out: list[dict] = []
    h1 = h1[h1.index + H1 <= now]
    if len(h1) < 6:
        return out
    ny = h1.tz_convert(NY_TZ)

    c0 = cur = prev = None
    for ts, o, h, l, c in zip(ny.index, ny["Open"], ny["High"], ny["Low"], ny["Close"]):
        pos = (ts.hour + 3) % 4                  # 0 = Q1 … 3 = Q4
        block = ts - timedelta(hours=pos)
        bar = {"ts": ts, "h": float(h), "l": float(l), "c": float(c), "block": block}
        if cur is None or block != cur["start"]:
            c0 = cur
            cur = {"start": block, "h": bar["h"], "l": bar["l"]}
        else:
            cur["h"] = max(cur["h"], bar["h"])
            cur["l"] = min(cur["l"], bar["l"])

        if pos == 1 and c0 is not None and prev is not None and prev["block"] == block:
            q1, q2 = prev, bar
            if q1["h"] > c0["h"] and q2["h"] > q1["h"] and q2["c"] < q1["h"]:
                out.append(_setup("short", c0, q1, q2))
            if q1["l"] < c0["l"] and q2["l"] < q1["l"] and q2["c"] > q1["l"]:
                out.append(_setup("long", c0, q1, q2))
        prev = bar
    return out


def _setup(direction: str, c0: dict, q1: dict, q2: dict) -> dict:
    short = direction == "short"
    return {
        "direction": direction,
        "c0_start": c0["start"],
        "c0_high": c0["h"],
        "c0_low": c0["l"],
        "q1_ts": q1["ts"],
        "q1_extreme": q1["h"] if short else q1["l"],
        "q2_ts": q2["ts"],
        "q2_extreme": q2["h"] if short else q2["l"],
        "q2_close": q2["c"],
        # Q2 a-t-elle déjà refermé dans le range de C0 ? Info, pas verrou.
        "back_in_range": q2["c"] < c0["h"] if short else q2["c"] > c0["l"],
        "target": c0["l"] if short else c0["h"],
        "q3_open_ts": q2["ts"] + H1,
    }


def fx_open(now: datetime) -> bool:
    """Forex coté : du dimanche 17h au vendredi 17h NY."""
    ny = now.astimezone(NY_TZ)
    wd, h = ny.weekday(), ny.hour
    if wd == 5:
        return False
    if wd == 6:
        return h >= 17
    if wd == 4:
        return h < 17
    return True


# ──────────────────────────────────────────────────────────────────
# Message
# ──────────────────────────────────────────────────────────────────

def _fmt(x: float) -> str:
    """3 décimales pour le DXY (~100), 5 pour les paires forex."""
    return f"{x:.3f}" if abs(x) >= 50 else f"{x:.5f}"


def _h(ts) -> str:
    return f"{ts.astimezone(NY_TZ).hour}h"


def format_alert(sym: str, s: dict, now: datetime) -> str:
    short = s["direction"] == "short"
    side = "HIGH" if short else "LOW"
    lines = [
        f"🧪 ARA'S MODEL — {sym} {'SHORT ↓' if short else 'LONG ↑'}",
        f"🕐 {now.astimezone(NY_TZ):%d/%m %Hh%M} NY · entrée à l'open de Q3 "
        f"({_h(s['q3_open_ts'])} NY)",
        "",
        f"📦 C0 ({_h(s['c0_start'])}–{_h(s['c0_start'] + timedelta(hours=4))} NY) : "
        f"H {_fmt(s['c0_high'])} · L {_fmt(s['c0_low'])}",
        f"1️⃣ Q1 ({_h(s['q1_ts'])}) prend le {side} de C0 → {_fmt(s['q1_extreme'])}",
        f"2️⃣ Q2 ({_h(s['q2_ts'])}) TS de Q1 : mèche {_fmt(s['q2_extreme'])}, "
        f"clôture {_fmt(s['q2_close'])}",
        "✅ Q2 a déjà refermé dans le range de C0" if s["back_in_range"] else
        f"⚠️ Q2 clôture encore {'au-dessus du high' if short else 'sous le low'} de C0",
        f"🛑 Extrême de Q2 : {_fmt(s['q2_extreme'])}",
        f"🎯 {'Low' if short else 'High'} de C0 (CRT) : {_fmt(s['target'])}",
    ]
    if now >= s["q3_open_ts"] + timedelta(minutes=30):
        lines.append("⏳ Envoyé en retard : Q3 est déjà bien entamée")
    return "\n".join(lines)


def send_telegram(text: str) -> bool:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        log("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID absents")
        return False
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(
                f"https://api.telegram.org/bot{token}/sendMessage", data=data, timeout=20
            ) as r:
                if json.loads(r.read()).get("ok"):
                    return True
        except Exception as e:
            log(f"Telegram KO (essai {attempt}/3) : {str(e)[:160]}")
        time.sleep(2 * attempt)
    return False


# ──────────────────────────────────────────────────────────────────
# Runs
# ──────────────────────────────────────────────────────────────────

def _load_seen() -> dict:
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _save_seen(seen: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(seen, indent=2), encoding="utf-8")


def scan(dry_run: bool = False) -> int:
    now = datetime.now(timezone.utc)
    if not fx_open(now):
        log("forex fermé — rien à scanner")
        return 0
    seen = _load_seen()
    errors = 0
    for sym, instrument in SYMBOLS.items():
        try:
            h1 = fetch_h1(instrument, now, days=4)
        except Exception as e:
            log(f"{sym} : {e}")
            errors += 1
            continue
        live = [s for s in find_setups(h1, now) if now < s["q3_open_ts"] + H1]
        if not live:
            log(f"{sym} : pas de setup en cours (dernière H1 {h1.index[-1]:%d/%m %H:%M}Z)")
            continue
        for s in live:
            key = f"{sym}:{s['direction']}"
            q2 = s["q2_ts"].isoformat()
            if seen.get(key) == q2:
                log(f"{sym} : {s['direction']} Q2 {q2} déjà envoyé")
                continue
            msg = format_alert(sym, s, now)
            if dry_run:
                print(msg + "\n")
                continue
            if send_telegram(msg):
                seen[key] = q2
                log(f"{sym} : {s['direction']} Q2 {q2} envoyé")
            else:
                errors += 1
    if not dry_run:
        _save_seen(seen)
    # Échec de toutes les paires = run rouge sur GitHub (email d'échec).
    return 1 if errors == len(SYMBOLS) else 0


def history(days: int) -> None:
    now = datetime.now(timezone.utc)
    for sym, instrument in SYMBOLS.items():
        setups = find_setups(fetch_h1(instrument, now, days=days), now)
        print(f"── {sym} : {len(setups)} setups sur {days} j")
        for s in setups:
            print(f"  {s['q2_ts']:%a %d/%m %Hh} NY  {s['direction']:5}  "
                  f"C0 H {_fmt(s['c0_high'])} L {_fmt(s['c0_low'])}  "
                  f"Q2 {_fmt(s['q2_extreme'])} → clôt {_fmt(s['q2_close'])}"
                  f"{'  (dans C0)' if s['back_in_range'] else ''}")


def test_message() -> int:
    """Message de test : le dernier setup GBPUSD trouvé, marqué TEST."""
    now = datetime.now(timezone.utc)
    setups = find_setups(fetch_h1(SYMBOLS["GBPUSD"], now, days=10), now)
    body = format_alert("GBPUSD", setups[-1], setups[-1]["q3_open_ts"]) if setups else ""
    text = ("✅ TEST — le bot Ara's Model tourne sur GitHub Actions.\n"
            "Exemple avec le dernier setup GBPUSD :\n\n" + body)
    ok = send_telegram(text)
    log("test envoyé" if ok else "test KO")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description="Bot Ara's Model → Telegram")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--history", type=int, metavar="JOURS")
    ap.add_argument("--test", action="store_true")
    args = ap.parse_args()
    if args.history:
        history(args.history)
        return 0
    if args.test:
        return test_message()
    return scan(dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
