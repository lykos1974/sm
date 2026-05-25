# PnF MVP - Real-Time Standalone Prototype

Αυτό είναι ένα **πρώτο λειτουργικό MVP** για real-time Point & Figure δοκιμές.

## Τι κάνει
- Τρέχει τοπικά σαν desktop εφαρμογή.
- Τραβάει live 1-minute κεριά από Binance public API.
- Αποθηκεύει candles σε SQLite.
- Χτίζει Point & Figure columns με fixed box size + reversal.
- Δείχνει scanner για λίγα symbols.
- Αποθηκεύει P&F state, columns και signal history.
- Έχει export των columns σε CSV.

## Τι ΔΕΝ είναι ακόμα
- Δεν είναι τελικό εμπορικό προϊόν.
- Δεν έχει ακόμα multi-exchange orchestration.
- Δεν έχει watchlist editor μέσα στο UI.
- Δεν έχει advanced alerts, replay και full backtest.
- Το signal engine είναι ακόμα βασικό: double top / double bottom.

## Αρχεία
- `app.py` -> κύριο desktop app
- `pnf_engine.py` -> λογική P&F
- `datafeed.py` -> live feed polling
- `storage.py` -> SQLite persistence
- `settings.json` -> ρυθμίσεις

## Προϋποθέσεις
Χρειάζεσαι μόνο **Python 3.11+** σε Windows.
Δεν υπάρχουν εξωτερικά πακέτα.

## Εκκίνηση
Άνοιξε PowerShell μέσα στον φάκελο και τρέξε:

```powershell
python app.py
```

## Τι να ελέγξεις πρώτα
1. Ανοίγει το παράθυρο.
2. Γεμίζει ο scanner πίνακας.
3. Ενημερώνονται οι τιμές κάθε λίγα δευτερόλεπτα.
4. Δημιουργείται το αρχείο `data/pnf_mvp.sqlite3`.
5. Μπορείς να κάνεις export columns CSV.

## Σημαντική σημείωση για P&F
Το engine εδώ είναι **σωστό MVP πυρήνα**, αλλά όχι ακόμα πλήρες institutional-grade P&F engine.
Είναι φτιαγμένο ώστε να ξεκινήσει άμεσα testing και μετά να επεκταθεί.

## Επόμενα σωστά βήματα
1. Έλεγχος ότι η λογική box/reversal συμφωνεί με αυτά που θέλεις.
2. Προσθήκη profile ανά symbol.
3. Ιστορικό preload στην εκκίνηση από API.
4. Πιο ισχυρό scanner score.
5. Alerts panel.
6. Replay mode.
7. Υποστήριξη παραπάνω feeds/exchanges.

## Binance Demo Forward Trader (positive-expectancy candidates only)

Χρησιμοποίησε το παρακάτω command για **DEMO / shadow** deployment μόνο:

```bash
LIVE_TRADING_ENABLED=0 \
python live_binance_forward_trader.py \
  --demo \
  --dry-run \
  --db-path "H:\\pnf screener\\market_collector\\market_collector\\market_collector\\market_data.db" \
  --state-db-path data/live_binance_forward_state_demo.sqlite3 \
  --settings pnf_mvp/settings.binance_demo_positive_expectancy.json \
  --history-bars 5000 \
  --loop \
  --poll-seconds 30
```

Required env vars when/if you want Binance Demo API connectivity checks:
- `BINANCE_DEMO_FUTURES_API_KEY`
- `BINANCE_DEMO_FUTURES_API_SECRET`

Forced demo self-test dry-run command:
```powershell
$env:LIVE_TRADING_ENABLED="0"
python live_binance_forward_trader.py --demo --dry-run --force-demo-order --force-demo-symbol BINANCE_FUT:SOLUSDT --db-path "<collector_db>" --state-db-path data/live_binance_forward_state_demo.sqlite3 --settings pnf_mvp/settings.binance_demo_positive_expectancy.json
```

Safety:
- Demo only.
- No real trading by default.
- ETH/BNB/XRP περιλαμβάνονται μόνο για demo signal-density observation, όχι ως proven positive expectancy.
- Παρακολούθηση logs συνεχώς.
- Μην θεωρείς ότι υπάρχει production edge χωρίς νέα validation.


## Pole diagnostics note
Pole patterns (High Pole / Low Pole) are **reversal diagnostics only** and must not be treated as production entries without dedicated validation.
