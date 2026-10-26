# Bot Ara's Model 🧪

Scan GBPUSD / EURUSD / DXY sur GitHub Actions (indépendant du Mac) et message Telegram quand l'Ara's Model apparaît :

- **C0** = bloc H4 qui vient de fermer (grille NY 1/5/9/13/17/21 h)
- **Q1** (1re H1 du bloc suivant) prend le high ou le low de C0
- **Q2** fait turtle soup de Q1 (mèche au-delà de Q1, clôture revenue en deçà)
- → entrée à l'open de **Q3**

Le message part pendant Q3 (en général 5 à 15 min après l'open, selon le retard du planificateur GitHub).

## Commandes

- Test Telegram : onglet **Actions** → *Ara's Model scan* → *Run workflow* → mode `test`.
- En local : `python ara_bot.py --dry-run` ou `python ara_bot.py --history 30`.

Secrets du repo : `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`.

Même règle que `~/tv-700s-mac/pine/ara_model.pine` et `~/tv-700s-mac/src/tv_700s_mac/watcher/ara_alert.py`.
