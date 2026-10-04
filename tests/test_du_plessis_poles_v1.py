from dataclasses import dataclass

import unittest

from pnf_mvp.strategies.du_plessis_poles_v1 import (
    PoleDecisionLedger, evaluate_prefix,
)


@dataclass
class C:
    idx: int
    kind: str
    top: int
    bottom: int
    start_ts: int
    end_ts: int


def high(top=19, retrace_bottom=15, box=1):
    return [C(0, 'X', 10*box, 7*box, 1, 1),
            C(1, 'O', 9*box, 7*box, 2, 2),
            C(2, 'X', 10*box, 8*box, 3, 3),
            C(3, 'O', 9*box, 8*box, 4, 4),
            C(4, 'X', top*box, 10*box, 5, 5),
            C(5, 'O', (top-1)*box, retrace_bottom*box, 6, 6)]


def eval_(cols, **kwargs):
    return evaluate_prefix(cols, box_size=kwargs.pop('box_size', 1),
                           reversal_boxes=3, enabled=True, **kwargs)


class PoleTests(unittest.TestCase):
    def test_off_and_exact_half_ten_five(self):
        c = high(top=19, retrace_bottom=14)
        assert evaluate_prefix(c, box_size=1, reversal_boxes=3) == ()
        e = eval_(c)[0]
        assert (e.column_length_boxes, e.retracement_boxes, e.breakout_excess_boxes) == (10, 5, 9)
        assert e.status == 'CANDIDATE' and not e.structural_label
        assert e.theoretical_trigger_level == '14'
        assert eval_(high(top=19, retrace_bottom=15)) == ()
        assert eval_(high(top=19, retrace_bottom=13))[0].structural_label


    def test_breakout_boundary_and_consolidation(self):
        assert eval_(high(top=13, retrace_bottom=10))[0].breakout_excess_boxes == 3
        assert eval_(high(top=12, retrace_bottom=9)) == ()
        c = high(top=19, retrace_bottom=14)
        c[1].bottom = 1
        c[1].top = 2
        assert eval_(c) == ()
        c = high(top=19, retrace_bottom=14)
        c[5].idx = 8
        assert eval_(c) == ()


    def test_odd_length_and_scaled_grid_and_reject_nonconstant(self):
        e = eval_(high(top=18, retrace_bottom=13), box_size=1)[0]
        assert (e.column_length_boxes, e.retracement_boxes) == (9, 5)
        e = eval_(high(top=19, retrace_bottom=14, box=25), box_size=25)[0]
        assert e.theoretical_trigger_level == '350'
        with self.assertRaisesRegex(ValueError, 'percentage/log'):
            eval_(high(top=19, retrace_bottom=14), box_size='1.01')


    def test_low_symmetry_exit_only_and_spot(self):
        c = high(top=19, retrace_bottom=14)
        for col in c:
            old_top, old_bottom = col.top, col.bottom
            col.top, col.bottom = 30-old_bottom, 30-old_top
            col.kind = 'O' if col.kind == 'X' else 'X'
        low = eval_(c)[0]
        assert low.pole_type == 'LOW_POLE' and low.action == 'LONG'
        assert eval_(high(top=19, retrace_bottom=14), venue='SPOT')[0].status == 'BLOCKED_SPOT_SHORT'
        assert eval_(high(top=19, retrace_bottom=14), mode='EXIT_ONLY')[0].status == 'POSITION_NOT_OWNED'
        assert eval_(high(top=19, retrace_bottom=14), mode='EXIT_ONLY', owned_position='LONG')[0].action == 'EXIT_LONG'


    def test_live_prefix_dedupe_reversal_requires_fill_and_no_auto_entry(self):
        c = high(top=19, retrace_bottom=14)
        ledger = PoleDecisionLedger()
        first = ledger.ingest(c, box_size=1, reversal_boxes=3, enabled=True)
        assert len(first) == 1 and ledger.ingest(c, box_size=1, reversal_boxes=3, enabled=True) == ()
        c.append(C(6, 'X', 17, 15, 7, 7))
        assert ledger.ingest(c, box_size=1, reversal_boxes=3, enabled=True) == ()
        with self.assertRaises(ValueError):
            ledger.acknowledge_simulated_fill(first[0].event_id, direction='SHORT', exchange_fill_ts=6, evidence_id='')
        ledger.acknowledge_simulated_fill(first[0].event_id, direction='SHORT', exchange_fill_ts=7, evidence_id='synthetic')
        c[-1].end_ts = 8
        exit_ = ledger.ingest(c, box_size=1, reversal_boxes=3, enabled=True)
        assert len(exit_) == 1 and exit_[0].action == 'CLOSE_SHORT'
        assert ledger.ingest(c, box_size=1, reversal_boxes=3, enabled=True) == ()
        assert len(ledger.events) == 2

class PrefixTests(unittest.TestCase):
    def test_prefix_invariance_restart_and_no_retroactive_fill(self):
        base = high(top=19, retrace_bottom=14)
        a = PoleDecisionLedger()
        assert a.ingest(base[:-1], box_size=1, reversal_boxes=3, enabled=True) == ()
        first = a.ingest(base, box_size=1, reversal_boxes=3, enabled=True)[0]
        extended = base + [C(6, 'X', 17, 15, 7, 7)]
        assert a.ingest(extended, box_size=1, reversal_boxes=3, enabled=True) == ()
        assert a.events[first.event_id] == first
        restarted = PoleDecisionLedger()
        assert restarted.ingest(base, box_size=1, reversal_boxes=3, enabled=True)[0] == first
        assert restarted.ingest(extended, box_size=1, reversal_boxes=3, enabled=True) == ()
        with self.assertRaises(ValueError):
            a.acknowledge_simulated_fill(first.event_id, direction='SHORT', exchange_fill_ts=first.decision_ts,
                               evidence_id='invalid-same-candle')

    def test_no_nonadjacent_or_unowned_exit_or_spot_short(self):
        c = high(top=19, retrace_bottom=14)
        ledger = PoleDecisionLedger(mode='EXIT_ONLY')
        event = ledger.ingest(c, box_size=1, reversal_boxes=3, enabled=True)[0]
        assert event.status == 'POSITION_NOT_OWNED'
        with self.assertRaises(ValueError):
            ledger.acknowledge_simulated_fill(event.event_id, direction='SHORT', exchange_fill_ts=7,
                                    evidence_id='synthetic')
        assert PoleDecisionLedger(venue='SPOT').ingest(c, box_size=1, reversal_boxes=3,
                                                       enabled=True)[0].status == 'BLOCKED_SPOT_SHORT'

class IntegrationBoundaryTests(unittest.TestCase):
    def test_restored_event_identity_and_frozen_first_timestamp(self):
        c = high(top=19, retrace_bottom=14)
        first = PoleDecisionLedger().ingest(c, box_size=1, reversal_boxes=3, enabled=True)[0]
        c[-1].bottom = 12
        c[-1].end_ts = 9
        ledger = PoleDecisionLedger(prior_events=[first])
        assert ledger.ingest(c, box_size=1, reversal_boxes=3, enabled=True) == ()
        assert ledger.events[first.event_id].decision_ts == 6
        with self.assertRaises(ValueError):
            PoleDecisionLedger(prior_events=[first, first])

    def test_baseline_and_old_detector_are_not_imported_or_mutated(self):
        from pathlib import Path
        source = Path('pnf_mvp/strategies/du_plessis_poles_v1.py').read_text()
        assert 'strategy_validation' not in source and 'live_mexc' not in source
        assert 'detect_pole_patterns' not in source
        from research_v2.backtest_workspace import EXECUTABLE
        assert EXECUTABLE == frozenset({'causal_long_pole'})

class PreviewTests(unittest.TestCase):
    def test_bounded_closed_candle_preview_without_fills(self):
        import tempfile
        from pathlib import Path
        from research_v2.du_plessis_poles_preview import preview
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'candles.csv'
            path.write_text('close_time,close\n' + ''.join(
                f'{i},{price}\n' for i, price in enumerate((7, 10, 7, 10, 7, 19, 13), 1)))
            result = preview(path, box_size=1, reversal_boxes=3,
                             mode='EARLY_ENTRY', venue='FUTURES', max_candles=7)
            assert result['execution'] == 'OFF'
            assert result['candles_processed'] == 7
            assert len(result['events']) == 1
            assert result['events'][0]['decision_ts'] == 7
            assert result['events'][0]['action'] == 'SHORT'
            assert preview(path, box_size=1, reversal_boxes=3, mode='EARLY_ENTRY',
                           venue='FUTURES', max_candles=6)['events'] == []

class ConsolidationExtremeTests(unittest.TestCase):
    def test_older_extreme_within_consolidation_blocks_false_breakout(self):
        c = high(top=13, retrace_bottom=10)
        c[1].top = 12  # adjacent O's upper level exceeded nearest X high
        c[1].bottom = 8
        assert eval_(c) == ()
