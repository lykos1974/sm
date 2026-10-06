"""Nonconsecutive harmonic projection is separate from Gartley v1 and S/R."""
import unittest

from research_v2.pnf_multicolumn_harmonic import project_multicolumn


def pivots():
    prices=(1000,6000,4000,11000,7000,8000,5000,7000,6000,10000)
    return [{"type":"PIVOT_CONFIRMED", "pivot_id":f"pnf:{i}:{'X' if i%2 else 'O'}",
             "column_id":i, "kind":"HIGH" if i%2 else "LOW", "price":str(price),
             "extreme_at":1000+i*100, "confirmed_at":1100+i*100,
             "confirmation_sequence":i+1,"confirmation_close":"7000"}
            for i,price in enumerate(prices)]


class MultiColumnHarmonicTests(unittest.TestCase):
    def test_valid_coarse_replay_may_start_with_x(self):
        facts=pivots()
        for fact in facts:
            kind="O" if fact["kind"]=="HIGH" else "X"
            fact["kind"]="HIGH" if kind=="X" else "LOW"
            fact["pivot_id"]=f"pnf:{fact['column_id']}:{kind}"
        self.assertIsInstance(project_multicolumn(facts),list)
        facts[4]["kind"]="LOW"  # invalid repeated O column
        with self.assertRaises(ValueError):
            project_multicolumn(facts)

    def test_nonconsecutive_xabc_projection_and_prefix(self):
        facts=pivots()
        found=project_multicolumn(facts)
        chosen=next(z for z in found if z["pivot_columns"]==[0,3,6,9])
        self.assertEqual((chosen["lower"],chosen["upper"],chosen["direction"]),
                         ("3140.000","4000","LONG"))
        self.assertEqual(chosen["known_at"],facts[9]["confirmed_at"])
        self.assertEqual(project_multicolumn(facts[:-1]),[z for z in found if z["known_at"]<chosen["known_at"]])

    def test_crossing_endpoint_and_late_projection_rejected(self):
        facts=pivots()
        facts[1]["price"]="12000"  # beyond the selected A on X→A.
        self.assertNotIn([0,3,6,9],[z["pivot_columns"] for z in project_multicolumn(facts)])
        facts=pivots();facts[9]["confirmation_close"]="3000"
        self.assertNotIn([0,3,6,9],[z["pivot_columns"] for z in project_multicolumn(facts)])

    def test_bad_numeric_and_reordered_evidence_fails_closed(self):
        for field,value in (("price","NaN"),("price",True),("confirmed_at",1),
                            ("column_id",False),("confirmation_close",1.5)):
            facts=pivots();facts[4][field]=value
            with self.subTest(field=field,value=value),self.assertRaises(ValueError):
                project_multicolumn(facts)


if __name__=="__main__":unittest.main()
