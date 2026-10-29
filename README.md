# Bot Ara's Model 🧪

Scan GBPUSD / EURUSD / DXY / BTCUSDT sur GitHub Actions (indépendant du Mac) et message Telegram dès que l'Ara's Model apparaît :

- **C0** = bougie H4 qui vient de fermer
- **Q1** (1re H1 de la bougie H4 suivante) prend le high ou le low de C0
- **Q2** fait turtle soup de Q1 (mèche au-delà de Q1, clôture revenue en deçà)
- → entrée à l'open de **Q3**

Les bougies sont celles des charts TradingView (heure NY) :

| Chart TV | Bougies H4 | Données du bot |
|---|---|---|
| ICMARKETS:GBPUSD / EURUSD | 17h01, 21h01, 1h01… (H1 à hh:01) | M1 Dukascopy agrégé avec 1 min de décalage |
| TVC:DXY | 19/23/3/7/11/15 h du mardi au vendredi, 17/21/1/5/9/13 h le lundi | DXY recalculé (formule ICE) depuis le M1 des 6 paires |
| BINANCE:BTCUSDT | 0/4/8/12/16/20 h UTC | klines Binance |

Un **veilleur** tourne en continu sur GitHub Actions : il se réveille 1 min avant chaque clôture de Q2, réclame la bougie jusqu'à ce qu'elle soit servie (~15 s) et envoie, puis se relaie lui-même toutes les ~5 h 30 (limite de 6 h d'un job). Le cron (toutes les 2 h) ne sert que de filet si la chaîne casse : le scheduler GitHub saute ou retarde la plupart de ses tirs. Un commit `keepalive` mensuel empêche GitHub de couper le cron (repo public inactif 60 jours).

## Commandes

- Test Telegram : onglet **Actions** → *Ara's Model scan* → *Run workflow* → mode `test`.
- Relancer le veilleur à la main : *Run workflow* → mode `watch`.
- En local : `python ara_bot.py --watch --dry-run`, `python ara_bot.py --dry-run` ou `python ara_bot.py --history 30`.

Secrets du repo : `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`.

Même règle que l'indicateur `~/tv-700s-mac/pine/ara_model.pine`.
