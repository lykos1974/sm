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
