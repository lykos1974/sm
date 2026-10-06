"""Causal multi-column support/resistance research labels."""
import copy
import unittest

from research_v2.pnf_multicolumn_sr import derive_zones, derive_coarse_zones


def pivot(i, kind, price):
    return {"type": "PIVOT_CONFIRMED", "pivot_id": f"pnf:{i}:{'X' if kind == 'HIGH' else 'O'}",
            "column_id": i, "kind": kind, "price": str(price),
            "extreme_at": 1000+i*100, "confirmed_at": 1100+i*100,
            "confirmation_sequence": i+1}


class MultiColumnZones(unittest.TestCase):
    def test_nonadjacent_resistance_exists_before_later_decline(self):
        prefix = [pivot(0,"LOW",60000),pivot(1,"HIGH",68600),
                  pivot(2,"LOW",65400),pivot(3,"HIGH",68900)]
        a = derive_zones(prefix)
        self.assertEqual(len(a),1)
        self.assertEqual((a[0]["type"],a[0]["lower"],a[0]["upper"]),
                         ("RESISTANCE","68600","68900"))
        self.assertEqual(a[0]["known_at"],1400)
        suffix = prefix+[pivot(4,"LOW",59900)]
        self.assertEqual(derive_zones(suffix)[:1],a)

    def test_support_symmetry_and_distant_columns(self):
        facts=[pivot(0,"HIGH",70000),pivot(1,"LOW",60000),
               pivot(2,"HIGH",66000),pivot(3,"LOW",63000),
               pivot(4,"HIGH",67000),pivot(5,"LOW",60200)]
        zones=derive_zones(facts)
        self.assertEqual([(z["type"],z["first_column"],z["second_column"])
                          for z in zones],[("SUPPORT",1,5)])

    def test_reject_wide_and_shallow_pairs(self):
        wide=[pivot(0,"LOW",60000),pivot(1,"HIGH",68000),pivot(2,"LOW",65000),pivot(3,"HIGH",68500)]
        shallow=[pivot(0,"LOW",60000),pivot(1,"HIGH",64000),pivot(2,"LOW",63500),pivot(3,"HIGH",64100)]
        self.assertEqual(derive_zones(wide),[])
        self.assertEqual(derive_zones(shallow),[])

    def test_no_hindsight_or_mutable_pivots(self):
        facts=[pivot(0,"LOW",60000),pivot(1,"HIGH",68600),pivot(2,"LOW",65400),pivot(3,"HIGH",68900)]
        old=copy.deepcopy(facts)
        self.assertEqual(len(derive_zones(facts)),1)
        self.assertEqual(facts,old)
        corrupt=copy.deepcopy(facts);corrupt[3]["confirmed_at"]=corrupt[2]["confirmed_at"]
        with self.assertRaises(ValueError):derive_zones(corrupt)

    def test_explicit_lookback_and_no_long_range_resurrection(self):
        facts=[pivot(i,"HIGH" if i%2 else "LOW",70000+i*500 if i%2 else 60000-i*500)
               for i in range(85)]
        facts[-1]["price"]="60000"
        self.assertEqual(derive_zones(facts),[])

    def test_coarse_profile_captures_large_multicolumn_level(self):
        facts=[pivot(0,"LOW",60000),pivot(1,"HIGH",68000),
               pivot(2,"LOW",65000),pivot(3,"HIGH",69000)]
        self.assertEqual(derive_zones(facts),[])
        zones=derive_coarse_zones(facts)
        self.assertEqual(len(zones),1)
        self.assertEqual((zones[0]["type"],zones[0]["lower"],zones[0]["upper"]),
                         ("RESISTANCE","68000","69000"))
        self.assertEqual(zones,derive_coarse_zones(facts+[pivot(4,"LOW",59000)])[:1])


if __name__ == "__main__":unittest.main()
