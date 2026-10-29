"""Bot Ara's Model 🧪 — scan GU / EU / DXY / BTC dans le cloud, message Telegram.

Modèle d'Antoine (2026-10-26) : anticiper un CRT H4 avant sa clôture.

    C0 = bougie H4 qui vient de fermer (grille H4 du chart TV du symbole)
    C1 = bougie H4 suivante, découpée en 4 H1 : Q1 Q2 Q3 Q4
    Q1 prend le high (ou le low) de C0
    Q2 fait turtle soup de Q1 : mèche au-delà de l'extrême de Q1, clôture
       revenue en deçà
    → entrée à l'open de Q3 (pari : C1 fermera en CRT, retour dans C0)

Les bougies H4 sont celles que TradingView affiche pour chaque chart (demande
d'Antoine, 2026-10-26) : TV les cale sur la session du symbole, qui diffère
d'un actif à l'autre — voir SESSIONS.

Tourne sur GitHub Actions (`.github/workflows/ara.yml`), indépendant du Mac.
Antoine veut le message DÈS la clôture de Q2. Le cron GitHub ne tient pas la
cadence (26-29/10 : un tir toutes les 4 à 6 h, 10 à 50 min de retard), d'où le
mode veilleur (`--watch`) : un run reste éveillé ~5 h 30, se réveille 1 min
avant chaque clôture de Q2, réclame la H1 de Q2 jusqu'à ce qu'elle soit servie
(Dukascopy : ~15 s après la clôture), envoie, puis le workflow relance le
veilleur suivant. Un setup n'est signalé que tant que Q3 est ouverte ;
`state/seen.json` (conservé par actions/cache) évite le doublon.

Même règle que `~/tv-700s-mac/pine/ara_model.pine` : les garder alignés.

Usage :
    python ara_bot.py --watch         # veilleur (le run normal du workflow)
    python ara_bot.py                 # un scan + Telegram
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
    INSTRUMENT_FX_CROSSES_USD_SEK,
    INSTRUMENT_FX_MAJORS_EUR_USD,
    INSTRUMENT_FX_MAJORS_GBP_USD,
    INSTRUMENT_FX_MAJORS_USD_CAD,
    INSTRUMENT_FX_MAJORS_USD_CHF,
    INSTRUMENT_FX_MAJORS_USD_JPY,
)

NY_TZ = ZoneInfo("America/New_York")
H1 = timedelta(hours=1)
H4 = timedelta(hours=4)

# Grille H4 de chaque chart TV, lue sur le flux TradingView le 2026-10-26
# (champ `session` du symbole) :
#   "fx"  ICMARKETS:GBPUSD / EURUSD  "1701-1659"  → sessions de 17h01 NY :
#         bougies H4 17h01/21h01/1h01/5h01/9h01/13h01 NY, et les H1 aussi
#         ouvrent à hh:01 → reconstruites depuis le M1 avec 1 min de décalage
#   "dxy" TVC:DXY  "1900-1900:3456|1700F-1900:2"  → sessions de 19h NY du mardi
#         au vendredi, bougies 19/23/3/7/11/15 h NY ; celle du lundi part du
#         dimanche 17h, bougies 17/21/1/5/9/13 h NY puis 17h–19h (tronquée)
#   "utc" BINANCE:BTCUSDT  "24x7", Etc/UTC  → bougies 0/4/8/12/16/20 h UTC
SESSIONS = ("fx", "dxy", "utc")
SESSION_MINUTE = {"fx": 1, "dxy": 0, "utc": 0}   # minute d'ouverture des bougies TV

SYMBOLS = {
    "GBPUSD": {"session": "fx", "duka": INSTRUMENT_FX_MAJORS_GBP_USD},
    "EURUSD": {"session": "fx", "duka": INSTRUMENT_FX_MAJORS_EUR_USD},
    "DXY": {"session": "dxy", "dxy_basket": True},
    # Même source que le chart TV BINANCE:BTCUSDT. api.binance.com refuse les
    # IP américaines (runners GitHub) : data-api.binance.vision d'abord.
    "BTCUSDT": {"session": "utc", "binance": "BTCUSDT"},
}
BINANCE_HOSTS = ("https://data-api.binance.vision", "https://api.binance.com")

# DXY recalculé avec la formule ICE depuis ses 6 paires, en M1 Dukascopy
# agrégé en H1. Validé le 2026-10-26 contre TVC:DXY : écart moyen 0.002 sur
# les highs / lows, 0.006 au pire. Le DXY de Dukascopy (CFD) est décalé de
# ~0.18 et n'a aucune donnée entre 17h et 20h NY.
DXY_CONST = 50.14348112
DXY_BASKET = (
    (INSTRUMENT_FX_MAJORS_EUR_USD, -0.576),
    (INSTRUMENT_FX_MAJORS_USD_JPY, 0.136),
    (INSTRUMENT_FX_MAJORS_GBP_USD, -0.119),
    (INSTRUMENT_FX_MAJORS_USD_CAD, 0.091),
    (INSTRUMENT_FX_CROSSES_USD_SEK, 0.042),
    (INSTRUMENT_FX_MAJORS_USD_CHF, 0.036),
)

STATE_PATH = Path(__file__).resolve().parent / "state" / "seen.json"
FETCH_RETRIES = 3

# Le run guette la clôture de Q2 qui vient de passer (moins de POLL_MAX) ou
# celle qui arrive dans MAX_LEAD (le cron peut démarrer en avance et attendre).
MAX_LEAD = timedelta(minutes=int(os.environ.get("ARA_MAX_LEAD_MIN", "15")))
# Après la clôture, on réclame la H1 de Q2 toutes les POLL_S secondes,
# POLL_MAX au plus ; au-delà, le run de secours de Q3 prend le relais.
POLL_S = 10
POLL_MAX = timedelta(minutes=10)
# Durée d'un veilleur : sous la limite de 6 h d'un job GitHub, marge comprise
# pour finir la dernière clôture (POLL_MAX) et passer le relais.
WATCH_MAX = timedelta(minutes=int(os.environ.get("ARA_WATCH_MAX_MIN", "330")))


def log(msg: str) -> None:
    print(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}Z {msg}", flush=True)


# ──────────────────────────────────────────────────────────────────
# Grille H4 TV
# ──────────────────────────────────────────────────────────────────

def session_start(kind: str, ts: pd.Timestamp) -> pd.Timestamp:
    """Ouverture de la session TV qui contient `ts` (heure murale du symbole)."""
    if kind == "utc":
        return ts.tz_convert("UTC").normalize()
    ny = ts.tz_convert(NY_TZ)
    anchor, minute = (17 if kind == "fx" else 19), SESSION_MINUTE[kind]
    s = ny.replace(hour=anchor, minute=minute, second=0, microsecond=0)
    if ny < s:
        s = (ny - timedelta(days=1)).replace(hour=anchor, minute=minute, second=0, microsecond=0)
    if kind == "dxy" and s.weekday() in (5, 6):
        # session du lundi : du dimanche 17h au lundi 19h
        sunday = s if s.weekday() == 6 else s + timedelta(days=1)
        s = sunday.replace(hour=17)
    return s


def block_start(kind: str, ts: pd.Timestamp) -> pd.Timestamp:
    """Ouverture de la bougie H4 TV qui contient `ts`. Pas de passage d'heure
    été / hiver dans une session (il tombe le dimanche 2h, marché fermé)."""
    s = session_start(kind, ts)
    return s + H4 * int((ts - s) / H4)


def market_open(kind: str, ts: pd.Timestamp) -> bool:
    """La H1 qui ouvre à `ts` existe-t-elle sur le chart TV ? Forex du dimanche
    17h au vendredi 17h NY ; TVC:DXY pareil mais à partir de 18h le dimanche ;
    BTC 24/7."""
    if kind == "utc":
        return True
    ny = ts.tz_convert(NY_TZ)
    wd, h = ny.weekday(), ny.hour
    if wd == 5:
        return False
    if wd == 6:
        return h >= (18 if kind == "dxy" else 17)
    if wd == 4:
        return h < 17
    return True


def is_q2_close(kind: str, t: pd.Timestamp) -> bool:
    """`t` (hh:00, ou hh:01 en forex) clôt une Q2 : la H1 de t-1h est la 2e H1
    cotée de sa bougie H4 (la bougie du dimanche soir du DXY commence à 18h)."""
    q2 = t - H1
    if not market_open(kind, q2):
        return False
    block = block_start(kind, q2)
    before = sum(market_open(kind, block + k * H1) for k in range(int((q2 - block) / H1)))
    return before == 1


# ──────────────────────────────────────────────────────────────────
# Données
# ──────────────────────────────────────────────────────────────────

def _retry(what: str, fn):
    last_err: Exception | None = None
    for attempt in range(1, FETCH_RETRIES + 1):
        try:
            return fn()
        except Exception as e:
            last_err = e
            log(f"{what} KO (essai {attempt}/{FETCH_RETRIES}) : {str(e)[:160]}")
            time.sleep(3 * attempt)
    raise RuntimeError(f"{what} indisponible : {last_err}")


def _fetch_duka(instrument, end: datetime, days: int,
                interval=dukascopy_python.INTERVAL_HOUR_1) -> pd.DataFrame:
    """Bougies Dukascopy (bid) closes uniquement : Dukascopy ne sert pas la
    bougie en cours (vérifié en H1 le 2026-10-26)."""
    def go():
        df = dukascopy_python.fetch(instrument, interval, dukascopy_python.OFFER_SIDE_BID,
                                    end - timedelta(days=days), end, max_retries=1)
        if df is None or df.empty:
            raise ValueError("série vide")
        return df

    df = _retry("Dukascopy", go)
    if not isinstance(df.index, pd.DatetimeIndex):
        if "timestamp" in df.columns:
            df = df.set_index("timestamp")
        df.index = pd.to_datetime(df.index)
    df.index = df.index.tz_localize("UTC") if df.index.tz is None else df.index.tz_convert("UTC")
    df = df.rename(columns={c: str(c).capitalize() for c in df.columns})
    return df[["Open", "High", "Low", "Close"]].sort_index()


def _fetch_dxy(end: datetime, days: int) -> pd.DataFrame:
    """DXY recalculé (formule ICE) en M1 puis agrégé en H1, aux heures où
    TVC:DXY cote. Le high d'une minute prend le côté de chaque paire qui
    pousse l'indice vers le haut (low des paires en EUR / GBP, high des autres)."""
    legs = [(_fetch_duka(inst, end, days, dukascopy_python.INTERVAL_MIN_1), w)
            for inst, w in DXY_BASKET]
    served = min(df.index[-1] for df, _ in legs) + timedelta(minutes=1)
    idx = legs[0][0].index
    for df, _ in legs[1:]:
        idx = idx.union(df.index)
    legs = [(df.reindex(idx).ffill(), w) for df, w in legs]

    def combine(up: str, down: str) -> pd.Series:
        out = pd.Series(DXY_CONST, index=idx)
        for df, w in legs:
            out = out * df[up if w > 0 else down] ** w
        return out

    m1 = pd.DataFrame({"Open": combine("Open", "Open"), "High": combine("High", "Low"),
                       "Low": combine("Low", "High"), "Close": combine("Close", "Close")}).dropna()
    return _h1_from_m1(m1, served, "dxy")


def _fetch_fx(instrument, end: datetime, days: int) -> pd.DataFrame:
    """H1 IC Markets telles que TV les dessine : de hh:01 à hh+1:01, depuis le M1."""
    m1 = _fetch_duka(instrument, end, days, dukascopy_python.INTERVAL_MIN_1)
    return _h1_from_m1(m1, m1.index[-1] + timedelta(minutes=1), "fx")


def _h1_from_m1(m1: pd.DataFrame, served: pd.Timestamp, kind: str) -> pd.DataFrame:
    """Agrège le M1 en H1 calées sur la minute d'ouverture TV de `kind`, aux
    heures où le chart TV a des bougies. `served` = fin du M1 complet."""
    m1 = m1[m1.index < served]
    h1 = (m1.resample("1h", offset=f"{SESSION_MINUTE[kind]}min")
            .agg({"Open": "first", "High": "max", "Low": "min", "Close": "last"})
            .dropna())
    h1 = h1[[market_open(kind, t) for t in h1.index]]
    h1.attrs["served_until"] = served
    return h1


def _fetch_binance(pair: str, end: datetime, days: int) -> pd.DataFrame:
    """Klines 1h Binance. La dernière est la bougie en cours (filtrée ensuite)."""
    start_ms = int((end - timedelta(days=days)).timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)

    def get(url: str) -> list:
        for host in BINANCE_HOSTS:
            try:
                with urllib.request.urlopen(host + url, timeout=20) as r:
                    return json.loads(r.read())
            except Exception as e:
                last = e
        raise last

    rows: list = []
    cursor = start_ms
    while cursor < end_ms:
        q = f"/api/v3/klines?symbol={pair}&interval=1h&startTime={cursor}&endTime={end_ms}&limit=1000"
        batch = _retry("Binance", lambda: get(q))
        if not batch:
            break
        rows += batch
        cursor = batch[-1][0] + 3_600_000
        if len(batch) < 1000:
            break
    if not rows:
        raise RuntimeError("Binance : série vide")
    df = pd.DataFrame([r[1:5] for r in rows], columns=["Open", "High", "Low", "Close"],
                      index=pd.to_datetime([r[0] for r in rows], unit="ms", utc=True)).astype(float)
    df = df[~df.index.duplicated()].sort_index()
    df.attrs["served_until"] = pd.Timestamp(end)      # temps réel
    return df


def fetch_h1(sym: str, end: datetime, days: int) -> pd.DataFrame:
    """H1 du symbole, index UTC, colonnes Open/High/Low/Close.
    `attrs["served_until"]` = instant jusqu'où les données sont complètes."""
    cfg = SYMBOLS[sym]
    if "binance" in cfg:
        return _fetch_binance(cfg["binance"], end, days)
    if cfg.get("dxy_basket"):
        return _fetch_dxy(end, days)
    return _fetch_fx(cfg["duka"], end, days)


# ──────────────────────────────────────────────────────────────────
# Détection
# ──────────────────────────────────────────────────────────────────

def find_setups(h1: pd.DataFrame, now: datetime, kind: str) -> list[dict]:
    """Tous les Ara's Model de la série, du plus ancien au plus récent.

    Bougie par bougie, comme l'indicateur Pine : chaque H1 est rangée dans la
    bougie H4 TV qui la contient (grille `kind`). Q1 / Q2 = 1re / 2e H1 de la
    bougie H4. C0 = la bougie H4 précédente présente dans la série (vendredi
    soir pour la première du dimanche). Seules les bougies closes et servies
    en entier à `now` (`attrs["served_until"]`) comptent.
    """
    out: list[dict] = []
    until = min(pd.Timestamp(now), h1.attrs.get("served_until", pd.Timestamp(now)))
    h1 = h1[h1.index + H1 <= until]
    if len(h1) < 6:
        return out

    c0 = cur = prev = None
    k = 0                                        # rang de la H1 dans sa bougie H4
    for ts, h, l, c in zip(h1.index, h1["High"], h1["Low"], h1["Close"]):
        block = block_start(kind, ts)
        bar = {"ts": ts, "h": float(h), "l": float(l), "c": float(c), "block": block}
        if cur is None or block != cur["start"]:
            c0 = cur
            cur = {"start": block, "end": ts + H1, "h": bar["h"], "l": bar["l"]}
            k = 0
        else:
            cur["end"] = ts + H1
            cur["h"] = max(cur["h"], bar["h"])
            cur["l"] = min(cur["l"], bar["l"])
            k += 1

        if k == 1 and c0 is not None and ts - prev["ts"] == H1:
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
        "c0_end": c0["end"],
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


# ──────────────────────────────────────────────────────────────────
# Message
# ──────────────────────────────────────────────────────────────────

def _fmt(x: float) -> str:
    """2 décimales pour BTC, 3 pour le DXY (~100), 5 pour les paires forex."""
    if abs(x) >= 1000:
        return f"{x:,.2f}".replace(",", " ")
    return f"{x:.3f}" if abs(x) >= 50 else f"{x:.5f}"


def _h(ts) -> str:
    """Heure NY : c'est le fuseau des charts TV d'Antoine."""
    return f"{ts.astimezone(NY_TZ).hour}h"


def format_alert(sym: str, s: dict, now: datetime) -> str:
    short = s["direction"] == "short"
    side = "HIGH" if short else "LOW"
    lines = [
        f"🧪 ARA'S MODEL — {sym} {'SHORT ↓' if short else 'LONG ↑'}",
        f"🕐 {now.astimezone(NY_TZ):%d/%m %Hh%M} NY · entrée à l'open de Q3 "
        f"({_h(s['q3_open_ts'])} NY)",
        "",
        f"📦 C0 ({_h(s['c0_start'])}–{_h(s['c0_end'])} NY) : "
        f"H {_fmt(s['c0_high'])} · L {_fmt(s['c0_low'])}",
        f"1️⃣ Q1 ({_h(s['q1_ts'])}) prend le {side} de C0 → {_fmt(s['q1_extreme'])}",
        f"2️⃣ Q2 ({_h(s['q2_ts'])}) TS de Q1 : mèche {_fmt(s['q2_extreme'])}, "
        f"clôture {_fmt(s['q2_close'])}",
        "✅ Q2 a déjà refermé dans le range de C0" if s["back_in_range"] else
        f"⚠️ Q2 clôture encore {'au-dessus du high' if short else 'sous le low'} de C0",
        f"🛑 Extrême de Q2 : {_fmt(s['q2_extreme'])}",
        f"🎯 {'Low' if short else 'High'} de C0 (CRT) : {_fmt(s['target'])}",
    ]
    late = now - s["q3_open_ts"]
    if late >= timedelta(minutes=3):
        lines.append(f"⏳ Envoyé {int(late.total_seconds() // 60)} min après la clôture de Q2")
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


def q2_targets(now: datetime) -> dict[str, pd.Timestamp]:
    """Clôture de Q2 à guetter par symbole : celle qui vient de passer (moins
    de POLL_MAX) ou celle qui arrive dans MAX_LEAD. Vide = run de secours."""
    now = pd.Timestamp(now)
    hour = now.floor("h")
    out = {}
    for sym, cfg in SYMBOLS.items():
        kind = cfg["session"]
        for t in (hour - H1, hour, hour + H1):
            t = t + timedelta(minutes=SESSION_MINUTE[kind])
            near = (t <= now and now - t < POLL_MAX) or (t > now and t - now <= MAX_LEAD)
            if near and is_q2_close(kind, t):
                out[sym] = t
                break
    return out


def _alert(sym: str, h1: pd.DataFrame, now: datetime, seen: dict, dry_run: bool) -> int:
    """Envoie les setups encore actionnables (Q3 ouverte). Retourne les échecs d'envoi."""
    served = h1.attrs["served_until"]
    live = [s for s in find_setups(h1, now, SYMBOLS[sym]["session"])
            if now < s["q3_open_ts"] + H1]
    if not live:
        log(f"{sym} : pas de setup (données jusqu'à {min(served, pd.Timestamp(now)):%d/%m %H:%M}Z)")
        return 0
    fails = 0
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
            fails += 1
    return fails


def scan(dry_run: bool = False) -> int:
    """Run du cron. Autour d'une clôture de Q2 : attend la clôture, réclame la
    H1 de Q2 jusqu'à ce qu'elle soit servie, puis envoie. Sinon (run de
    secours) : scan direct des symboles qui cotent."""
    now = datetime.now(timezone.utc)
    targets = q2_targets(now)
    if targets:
        symbols = list(targets)
        log("clôture de Q2 : " + ", ".join(f"{s} {t:%H:%M}Z" for s, t in targets.items()))
    else:
        symbols = [s for s, cfg in SYMBOLS.items()
                   if market_open(cfg["session"], pd.Timestamp(now) - H1)]
        if not symbols:
            log("marchés fermés — rien à scanner")
            return 0
        log(f"run de secours : {', '.join(symbols)}")

    seen = _load_seen()
    pending = list(symbols)
    errors = 0
    while pending:
        now = datetime.now(timezone.utc)
        waiting = [targets[s] for s in pending if s in targets and targets[s] > now]
        if len(waiting) == len(pending):          # rien de clos : on dort jusqu'à la 1re clôture
            time.sleep((min(waiting) - now).total_seconds() + 1)
            continue
        for sym in list(pending):
            close = targets.get(sym)
            if close is not None and close > now:
                continue
            try:
                h1 = fetch_h1(sym, now, days=4)
            except Exception as e:
                log(f"{sym} : {e}")
                errors += 1
                pending.remove(sym)
                continue
            q2_missing = close is not None and h1.attrs["served_until"] < close
            if q2_missing and now < close + POLL_MAX:
                continue                        # H1 de Q2 pas encore servie en entier
            pending.remove(sym)
            if q2_missing:
                log(f"{sym} : H1 de Q2 toujours pas servie — le run de secours repassera")
                continue
            if close is not None:
                log(f"{sym} : H1 de Q2 servie, {(now - close).total_seconds():.0f}s après la clôture")
            errors += _alert(sym, h1, now, seen, dry_run)
        if pending:
            time.sleep(POLL_S)
    if not dry_run:
        _save_seen(seen)
    # Échec sur tous les symboles = run rouge sur GitHub (email d'échec).
    return 1 if symbols and errors >= len(symbols) else 0


def next_q2_close(now: datetime) -> pd.Timestamp | None:
    """Prochaine clôture de Q2, tous symboles confondus, strictement après `now`."""
    now = pd.Timestamp(now)
    hour = now.floor("h")
    best = None
    for cfg in SYMBOLS.values():
        kind = cfg["session"]
        for k in range(9):                      # BTC clôt une Q2 toutes les 4 h
            t = hour + k * H1 + timedelta(minutes=SESSION_MINUTE[kind])
            if t > now and is_q2_close(kind, t):
                best = t if best is None else min(best, t)
                break
    return best


def watch(dry_run: bool = False) -> int:
    """Veilleur : un scan de rattrapage, puis un scan à chaque clôture de Q2
    jusqu'à WATCH_MAX. Rend la main juste après une clôture, quand la suivante
    tombe après la fin : le veilleur suivant a alors ~1 h pour démarrer."""
    deadline = datetime.now(timezone.utc) + WATCH_MAX
    log(f"veilleur jusqu'à {deadline:%d/%m %H:%M}Z")
    rc = 0
    t = None                                    # clôture visée (None = rattrapage au démarrage)
    while True:
        try:
            rc = scan(dry_run)
        except Exception as e:                  # un scan raté ne tue pas le veilleur
            log(f"scan KO : {type(e).__name__}: {str(e)[:200]}")
            rc = 1
        now = datetime.now(timezone.utc)
        if t is not None and now < t:           # jamais deux scans pour la même clôture
            time.sleep((t - now).total_seconds() + 1)
            now = datetime.now(timezone.utc)
        t = next_q2_close(now)
        if t is None or t > deadline:
            log(f"prochaine clôture de Q2 {t:%d/%m %H:%M}Z après la fin — relais au veilleur suivant"
                if t is not None else "aucune clôture de Q2 en vue — relais")
            return rc
        wake = t - timedelta(minutes=1)
        if wake > now:
            time.sleep((wake - now).total_seconds())


def history(days: int) -> None:
    now = datetime.now(timezone.utc)
    for sym, cfg in SYMBOLS.items():
        setups = find_setups(fetch_h1(sym, now, days=days), now, cfg["session"])
        print(f"── {sym} : {len(setups)} setups sur {days} j")
        for s in setups:
            print(f"  {s['q2_ts'].astimezone(NY_TZ):%a %d/%m %Hh} NY  {s['direction']:5}  "
                  f"C0 {_h(s['c0_start'])}–{_h(s['c0_end'])} H {_fmt(s['c0_high'])} "
                  f"L {_fmt(s['c0_low'])}  Q2 {_fmt(s['q2_extreme'])} → clôt {_fmt(s['q2_close'])}"
                  f"{'  (dans C0)' if s['back_in_range'] else ''}")


def test_message() -> int:
    """Message de test : le dernier setup BTC trouvé, marqué TEST."""
    now = datetime.now(timezone.utc)
    setups = find_setups(fetch_h1("BTCUSDT", now, days=10), now, "utc")
    body = format_alert("BTCUSDT", setups[-1], setups[-1]["q3_open_ts"]) if setups else ""
    text = ("✅ TEST — le bot Ara's Model tourne sur GitHub Actions (GU, EU, DXY, BTC).\n"
            "Exemple avec le dernier setup BTC :\n\n" + body)
    ok = send_telegram(text)
    log("test envoyé" if ok else "test KO")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description="Bot Ara's Model → Telegram")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--history", type=int, metavar="JOURS")
    ap.add_argument("--test", action="store_true")
    ap.add_argument("--watch", action="store_true", help="veilleur jusqu'à ARA_WATCH_MAX_MIN")
    args = ap.parse_args()
    if args.history:
        history(args.history)
        return 0
    if args.test:
        return test_message()
    if args.watch:
        return watch(dry_run=args.dry_run)
    return scan(dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
