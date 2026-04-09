BE experiment package

Files
-----
1. trade_management_be.py
   Standalone module with:
   - BE_MODE
   - BE_TRIGGER_R
   - fee-buffered BE price
   - bar-processing helpers for LONG / SHORT

Recommended test order
----------------------
A) Baseline:
   BE_MODE = False

B) Conservative:
   BE_MODE = True
   BE_TRIGGER_R = 1.5

C) Tighter:
   BE_MODE = True
   BE_TRIGGER_R = 1.0

Important
---------
This module is intentionally separate from strategy_engine.py.
Your profitable edge is currently in setup selection.
Breakeven belongs in trade-resolution / validation logic, not in the setup engine.
