# Standalone live collector: closed-candle fix

This directory versions the standalone collector supplied from the user's Windows installation. It is separate from `pnf_mvp` and its research backfill runners. Existing local `app.py` and `settings.json` remain the launch/configuration entry points; they are not copied or replaced by this change.

`storage.py` is the unchanged supplied collector storage implementation, included to make regression tests self-contained. Install only `collector.py` into the existing collector folder. Do not copy this storage implementation into the scanner folder.

## Behaviour

- Persist only candles whose close timestamp precedes the start of the symbol operation by at least 5 seconds (the scanner's existing grace period).
- Freeze this cutoff before fetching, so a slow response cannot turn an earlier provisional value into an apparently final candle just by waiting.
- Include the latest stored candle in each poll request. This finalizes a latest row left by the previous collector version and keeps repeat polls unique through the existing primary key.
- Preserve the committed candle watermark when no eligible rows arrive.
- Existing 1m configuration, symbol prefixes, API endpoints and database schema remain unchanged.

The local clock must be correct and the API must return final historical candles after close. This is not a guarantee against exchange-side later revisions. Existing older provisional rows, internal gaps and persisted scanner state are not repaired here. Bootstrap is not a repair operation. The separate autopilot is not changed in this PR.

## Verification

From the repository root:

```text
python -m unittest discover -s tests -p test_live_collector_finality.py -v
```

Five tests each cover Binance and MEXC: provisional bootstrap/final poll, latest-row repair and repeat polling, grace/empty-poll state, delayed response finality, and paginated overlap progress. Tests prohibit network calls and use temporary SQLite only. No strategy scorecard is recalculated because this is a feed correctness fix, not a strategy experiment. Strategy/promotion/SL/TP and the protected baseline source are untouched.

## Τοπική εγκατάσταση — χωρίς Git ή αλλαγή κώδικα με το χέρι

1. Κλείσε προσωρινά τον PnF scanner, τον Market Collector και τον ξεχωριστό autopilot, αν τρέχει.
2. Άνοιξε τον υπάρχοντα φάκελο:
   `H:\pnf screener\market_collector\market_collector\market_collector`
3. Κράτησε αντίγραφο του υπάρχοντος `collector.py` ως `collector_before_finality_fix.py.bak`.
4. Αντικατάστησε μόνο το `collector.py` με το πλήρες διορθωμένο αρχείο. Βεβαιώσου ότι δεν ονομάστηκε `collector(1).py` κατά τη λήψη. Μην αλλάξεις `app.py`, `storage.py`, `settings.json` ή οποιοδήποτε `.db`.
5. Ξεκίνησε όπως ήδη κάνεις, με `python app.py` από αυτόν τον φάκελο και πάτησε **Start live collector**. Μην πατήσεις **Bootstrap all history**.
6. Άφησε μόνο τον collector να τρέξει 2–3 λεπτά και στείλε το **Copy log**. Ο scanner και ο autopilot παραμένουν προσωρινά κλειστοί μέχρι τον επόμενο έλεγχο, επειδή το προηγούμενο ιστορικό/state δεν έχει αποκατασταθεί από αυτή τη διόρθωση.

Δεν χρειάζεται διαγραφή/επαναδημιουργία βάσης ή πολύωρο backfill. Το `updated N rows` μετρά upserts, επομένως μπορεί να επαναλαμβάνεται ενημέρωση του τελευταίου candle χωρίς νέο λεπτό. Μια υγιής ένδειξη λειτουργίας δεν πιστοποιεί ακόμη την ποιότητα όλου του παλιού ιστορικού.

Rollback του κώδικα: κλείσε τον collector και επανάφερε το αντίγραφο ως `collector.py`. Αυτό δεν αναιρεί candle updates που ήδη γράφτηκαν. Η παλιά έκδοση έχει το γνωστό finality bug και η επαναφορά δεν σημαίνει ότι τα δεδομένα γίνονται αξιόπιστα.

## Optional BTCUSDT microstructure collector

`microstructure_collector.py` is a separate read-only forward evidence tool. It
listens only to the public Binance Spot `btcusdt@bookTicker` and
`btcusdt@aggTrade` streams. It has no API key, sends no orders, and never opens
`market_data.db`.

Install its dependency and start it from this directory:

```text
python -m pip install -r requirements-microstructure.txt
python microstructure_collector.py
```

The dedicated default database is `microstructure_btcusdt.db`. It records local
wall/monotonic receive time, raw JSON, connection sessions, best bid/ask, and
aggregate trades. Possible aggregate-trade ID gaps are recorded in
`stream_anomalies`. Jumps in `bookTicker.updateId` are not called gaps because
the stream publishes best-quote changes, not every order-book update.

This evidence does not prove a hypothetical limit fill: it contains neither
queue position nor private order acknowledgements. It must remain separate from
strategy and validation until its quality is independently checked.

After a bounded collection run, audit the database without modifying it:

```text
python check_microstructure_db.py
```
