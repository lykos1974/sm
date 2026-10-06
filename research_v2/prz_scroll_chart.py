"""Offline, scrollable P&F view of the pinned 2024 PRZ decision ledger.

Usage: python -B -m research_v2.prz_scroll_chart --report REPORT.json
       --candles candles_1m.csv --output NEW.html
"""
from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import html
import json
import os
import tempfile
from pathlib import Path

from research_v2.du_plessis_poles_annual import (
    FIRST_CLOSE_MS, LAST_CLOSE_MS, SOURCE_MINUTES, SOURCE_SHA256,
)
from research_v2.du_plessis_poles_preview import PnFEngine, PnFProfile
from research_v2.gartley_pole_prz import number
from research_v2.pnf_multicolumn_sr import derive_coarse_zones


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def extract_zones(report: dict) -> list[dict]:
    if (report.get("schema") != "prz-x-break-pole-2024-exploratory-v1"
            or report.get("source_sha256") != SOURCE_SHA256
            or report.get("box_size") != "100"
            or report.get("reversal_boxes") != 3
            or report.get("research_only") is not True
            or report.get("execution") != "OFF"):
        raise ValueError("incompatible research report")
    facts = report.get("decision_facts")
    if not isinstance(facts, list):
        raise ValueError("missing decision facts")
    zones: dict[str, dict] = {}
    pivot_prices: dict[str, str] = {}
    events = {"ZONE_CONTACT", "ZONE_FULL_TEST", "ZONE_INVALIDATED", "ZONE_EXPIRED", "B_VIOLATION"}
    for fact in facts:
        if not isinstance(fact, dict) or not isinstance(fact.get("type"), str):
            raise ValueError("invalid decision fact")
        kind = fact["type"]
        if kind == "PIVOT_CONFIRMED":
            key = fact.get("pivot_id")
            if not isinstance(key, str) or key in pivot_prices:
                raise ValueError("duplicate pivot")
            pivot_prices[key] = str(number(fact["price"]))
        elif kind == "ZONE_CREATED":
            key = fact.get("candidate_id")
            if not isinstance(key, str) or key in zones:
                raise ValueError("duplicate zone")
            if fact.get("direction") not in ("LONG", "SHORT") or fact.get("state") not in ("WAITING", "UNAVAILABLE"):
                raise ValueError("invalid zone state")
            low, high = number(fact["lower"]), number(fact["upper"])
            at, column = fact.get("zone_known_at"), fact.get("connected_column_id")
            ids = fact.get("pivot_ids")
            if (low <= 0 or low > high or high-low > 100 or type(at) is not int
                    or type(column) is not int or column < 4
                    or not isinstance(ids, list) or len(ids) != 4
                    or any(pid not in pivot_prices for pid in ids)):
                raise ValueError("invalid zone geometry")
            zones[key] = {"id": key, "direction": fact["direction"],
                          "state": fact["state"], "lower": str(low), "upper": str(high),
                          "at": at, "column": column, "events": [],
                          "pivots": [{"column": int(pid.split(":")[1]), "price": pivot_prices[pid]}
                                     for pid in ids]}
            if [p["column"] for p in zones[key]["pivots"]] != list(range(column-4, column)):
                raise ValueError("nonconsecutive pivots")
        elif kind in events:
            key, at = fact.get("candidate_id"), fact.get("at")
            if key not in zones or type(at) is not int or at < zones[key]["at"]:
                raise ValueError("orphaned zone event")
            zones[key]["events"].append({"type": kind, "at": at})
    if len(zones) > 1000:
        raise ValueError("too many zones")
    return sorted(zones.values(), key=lambda z: (z["at"], z["id"]))


def replay_multiscale(path: Path) -> tuple[list[dict], list[dict]]:
    if sha256(path) != SOURCE_SHA256:
        raise ValueError("pinned candle SHA-256 mismatch")
    engine = PnFEngine(PnFProfile("prz_scroll_chart_offline", 100, 3))
    coarse = PnFEngine(PnFProfile("prz_structural_10x_offline", 1000, 3))
    coarse_pivots = []
    first = previous = None
    count = 0
    with path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames or not {"close_time", "close"} <= set(reader.fieldnames):
            raise ValueError("invalid candle columns")
        for row in reader:
            ts = int(row["close_time"])
            if previous is not None and ts != previous + 60000:
                raise ValueError("non-contiguous candles")
            close = number(row["close"])
            if close <= 0:
                raise ValueError("invalid candle close")
            engine.update_from_price(ts, float(close))
            coarse.update_from_price(ts, float(close))
            while len(coarse_pivots) < len(coarse.columns)-1:
                column = coarse.columns[len(coarse_pivots)]
                kind = "HIGH" if column.kind == "X" else "LOW"
                coarse_pivots.append({"type": "PIVOT_CONFIRMED",
                                      "pivot_id": f"pnf:{column.idx}:{column.kind}",
                                      "column_id": column.idx, "kind": kind,
                                      "price": str(number(str(column.top if kind == "HIGH" else column.bottom))),
                                      "extreme_at": column.end_ts,
                                      "confirmed_at": ts,
                                      "confirmation_sequence": count+1})
            first = ts if first is None else first
            previous = ts
            count += 1
            if count > SOURCE_MINUTES:
                raise ValueError("too many candles")
    if (first, previous, count) != (FIRST_CLOSE_MS, LAST_CLOSE_MS, SOURCE_MINUTES) or sha256(path) != SOURCE_SHA256:
        raise ValueError("incomplete or changed candle input")
    return ([{"idx": c.idx, "kind": c.kind, "top": c.top, "bottom": c.bottom,
              "start": c.start_ts, "end": c.end_ts} for c in engine.columns],
            coarse_pivots)


def replay_columns(path: Path) -> list[dict]:
    """Compatibility helper for callers needing only the 100-box chart."""
    return replay_multiscale(path)[0]


def map_coarse_zones(columns: list[dict], pivots: list[dict]) -> list[dict]:
    """Map coarse confirmed facts onto the frozen fine-chart time axis."""
    starts = [c["start"] for c in columns]
    def index(at: int) -> int:
        i = bisect.bisect_right(starts, at)-1
        if i < 0 or i >= len(columns):
            raise ValueError("coarse pivot outside fine chart")
        return i
    mapped = []
    for z in derive_coarse_zones(pivots):
        row = dict(z)
        row["coarse_first_column"], row["coarse_second_column"] = z["first_column"], z["second_column"]
        row["first_column"] = index(z["first_extreme_at"])
        row["second_column"] = index(z["second_extreme_at"])
        row["known_column"] = index(z["known_at"])
        if not row["first_column"] < row["second_column"] <= row["known_column"]:
            raise ValueError("coarse/fine chronology mismatch")
        mapped.append(row)
    return mapped


def chart_html(columns: list[dict], zones: list[dict], *, source_hash: str, report_hash: str,
               structural_zones: list[dict] | None = None) -> str:
    structural_zones = structural_zones or []
    if not columns or any(c["idx"] != i or c["kind"] not in ("X", "O") for i, c in enumerate(columns)):
        raise ValueError("invalid P&F columns")
    if any(z["column"] >= len(columns) or any(p["column"] >= len(columns) for p in z["pivots"]) for z in zones):
        raise ValueError("zone outside P&F chart")
    for z in structural_zones:
        if (z["known_column"] >= len(columns)
                or not columns[z["known_column"]]["start"] <= z["known_at"]
                or (z["known_column"]+1 < len(columns)
                    and z["known_at"] >= columns[z["known_column"]+1]["start"])
                or not z["first_column"] < z["second_column"] <= z["known_column"]):
            raise ValueError("structural zone chronology mismatch")
    data = json.dumps({"columns": columns, "zones": zones, "structural": structural_zones},
                      separators=(",", ":"), ensure_ascii=True).replace("<", "\\u003c")
    template = r'''<!doctype html><html lang="el"><meta charset="utf-8">
<title>BTCUSDT 2024 · PRZ στον P&amp;F χάρτη</title>
<style>
body{margin:0;background:#0d1420;color:#e9eff9;font:14px system-ui,Segoe UI,sans-serif}header{padding:14px 18px;background:#162338;position:sticky;top:0;z-index:2}
h1{font-size:20px;margin:0 0 8px}button,select{background:#263952;color:white;border:1px solid #55708e;border-radius:5px;padding:7px;margin-right:5px}
.row{display:flex;flex-wrap:wrap;gap:9px;align-items:center}.muted{color:#aebed1}.summary{padding:10px 18px}.viewport{overflow-x:auto;overflow-y:hidden;border-top:1px solid #40556e;border-bottom:1px solid #40556e}
.strip{height:640px;position:relative}.strip canvas{position:sticky;left:0;display:block}.detail{min-height:62px;padding:8px 18px;line-height:1.5}
.key{display:inline-block;width:10px;height:10px;margin:0 5px 0 12px}.long{background:#3bd6aa}.short{background:#fb8d83}.sr{background:#e6bd55}
</style>
<header><h1>BTCUSDT · όλες οι PRZ στο P&amp;F του 2024</h1><div class="row">
<button id="prev">◀ Προηγούμενη PRZ</button><select id="pick" aria-label="Επιλογή PRZ"></select><button id="next">Επόμενη PRZ ▶</button>
<button id="all">Αρχή χρονιάς</button><label>Μήνας <select id="month" aria-label="Μετάβαση σε μήνα"></select></label><label>Μεγέθυνση <input id="zoom" type="range" min="12" max="40" value="22"></label>
<label><input id="showLong" type="checkbox" checked> Ανοδικές PRZ</label><label><input id="showShort" type="checkbox" checked> Καθοδικές PRZ</label>
<label><input id="showSr" type="checkbox" checked> Επιλεγμένη δομική ζώνη</label><button id="prevSr">◀ Ζώνη</button><select id="pickSr" aria-label="Επιλογή δομικής ζώνης"><option value="">Επίλεξε δομική ζώνη…</option></select><button id="nextSr">Ζώνη ▶</button></div></header>
<div class="summary"><b id="counts"></b><span class="key long"></span>LONG Gartley PRZ <span class="key short"></span>SHORT Gartley PRZ <span class="key sr"></span>δομική στήριξη/αντίσταση (όχι harmonic PRZ) · ◆: γεγονός · οριζόντια κύλιση<br><b id="window"></b></div>
<div id="viewport" class="viewport"><div id="strip" class="strip"><canvas id="chart" aria-label="Κυλιόμενος P&F χάρτης με ζώνες PRZ"></canvas></div></div>
<div id="detail" class="detail" aria-live="polite"></div><div class="summary muted">Έρευνα μόνο · δομικές ζώνες από ξεχωριστή αδρή P&F κλίμακα 1000/3, μία κάθε φορά · δεν είναι harmonic PRZ ή σήμα εκτέλεσης. Η οριζόντια επέκταση κατά 20 λεπτές στήλες είναι μόνο για προβολή.<br>Κεριά SHA-256: SOURCE_HASH · Αναφορά SHA-256: REPORT_HASH</div>
<script id="dataset" type="application/json">DATA_JSON</script><script>
"use strict";const {columns:cols,zones,structural}=JSON.parse(document.getElementById('dataset').textContent);
const box=100,view=document.getElementById('viewport'),strip=document.getElementById('strip'),canvas=document.getElementById('chart'),ctx=canvas.getContext('2d');
const pick=document.getElementById('pick'),pickSr=document.getElementById('pickSr'),detail=document.getElementById('detail'),zoom=document.getElementById('zoom');let selected=0,selectedSr=-1,unit=22;
function colAt(ts){let lo=0,hi=cols.length;while(lo<hi){let mid=(lo+hi)>>1;if(cols[mid].start<=ts)lo=mid+1;else hi=mid}return Math.max(0,lo-1)}
function utc(ts){return new Date(ts).toISOString().replace('T',' ').slice(0,16)+' UTC'}
for(let i=0;i<zones.length;i++){let z=zones[i],o=document.createElement('option');o.value=i;o.textContent=`${i+1}. ${utc(z.at)} · ${z.direction} · ${z.lower}–${z.upper}`;pick.append(o)}
for(let i=0;i<structural.length;i++){let z=structural[i],o=document.createElement('option');o.value=i;o.textContent=`${i+1}. ${utc(z.known_at)} · ${z.type} · ${z.lower}–${z.upper}`;pickSr.append(o)}
const month=document.getElementById('month');for(let m=1;m<=12;m++){let o=document.createElement('option');o.value=m;o.textContent=`2024-${String(m).padStart(2,'0')}`;month.append(o)}
document.getElementById('counts').textContent=`${cols.length} P&F στήλες · ${zones.length} Gartley PRZ · ${structural.length} δομικές ζώνες · 2024 UTC`;
function visible(z){return z.direction==='LONG'?document.getElementById('showLong').checked:document.getElementById('showShort').checked}
function select(i){if(!zones.length)return;selected=(i+zones.length)%zones.length;pick.value=selected;let z=zones[selected];view.scrollLeft=Math.max(0,(z.column-12)*unit);showDetail();draw()}
function showDetail(){if(!zones.length){detail.textContent='Δεν καταγράφηκαν PRZ.';return}let z=zones[selected],events=z.events.map(e=>`${e.type.replace('ZONE_','')} ${utc(e.at)}`).join(' · ')||'Κανένα μεταγενέστερο γεγονός';detail.textContent=`PRZ ${selected+1}/${zones.length} · ${z.direction} · ${z.state} κατά τη δημιουργία · ${utc(z.at)} · περιοχή ${z.lower}–${z.upper} · X/A/B/C στήλες ${z.pivots.map(p=>p.column).join('/')} · ${events}`}
function selectSr(i){if(!structural.length)return;selectedSr=(i+structural.length)%structural.length;let z=structural[selectedSr];pickSr.value=selectedSr;view.scrollLeft=Math.max(0,(z.first_column-5)*unit);detail.textContent=`Δομική ${z.type} ${selectedSr+1}/${structural.length} · ${z.lower}–${z.upper} · αδρές στήλες ${z.coarse_first_column}/${z.coarse_second_column} · γνωστή από ${utc(z.known_at)} · ενδιάμεση κίνηση ${z.excursion_boxes} αδρά boxes · ΧΩΡΙΣ Fibonacci επιβεβαίωση`;draw()}
function draw(){let w=view.clientWidth,h=640,dpr=window.devicePixelRatio||1;if(canvas.width!==Math.round(w*dpr)){canvas.width=Math.round(w*dpr);canvas.height=Math.round(h*dpr);canvas.style.width=w+'px';canvas.style.height=h+'px'}ctx.setTransform(dpr,0,0,dpr,0,0);ctx.clearRect(0,0,w,h);ctx.fillStyle='#0d1420';ctx.fillRect(0,0,w,h);
let offset=view.scrollLeft,start=Math.max(0,Math.floor(offset/unit)-3),end=Math.min(cols.length,Math.ceil((offset+w)/unit)+3);if(end<=start)return;
document.getElementById('window').textContent=`Ορατό διάστημα: ${utc(cols[start].start)} έως ${utc(cols[end-1].end)} · η επιλεγμένη ζώνη μπορεί να βρίσκεται εκτός οθόνης`;
let relevant=zones.filter(z=>visible(z)&&z.column<=end&&colAt((z.events.find(e=>e.type==='ZONE_INVALIDATED'||e.type==='ZONE_EXPIRED')||{at:cols.at(-1).end}).at)>=start);
let srVisible=document.getElementById('showSr').checked&&selectedSr>=0?structural.filter((z,i)=>i===selectedSr&&z.first_column<=end&&z.known_column+20>=start):[];
let values=[];for(let i=start;i<end;i++){values.push(cols[i].bottom,cols[i].top)}for(let z of relevant)values.push(Number(z.lower),Number(z.upper));for(let z of srVisible)values.push(Number(z.lower),Number(z.upper));
let low=Math.floor((Math.min(...values)-box*2)/box)*box,high=Math.ceil((Math.max(...values)+box*2)/box)*box;
let top=32,bottom=h-47,y=p=>bottom-(p-low)/(high-low)*(bottom-top),x=i=>i*unit-offset+unit/2;
ctx.strokeStyle='#26364a';ctx.fillStyle='#94a9c1';ctx.font='11px system-ui';let skip=Math.max(1,Math.ceil((high-low)/box/24));for(let p=low,n=0;p<=high;p+=box,n++)if(n%skip===0){let yy=y(p);ctx.beginPath();ctx.moveTo(45,yy);ctx.lineTo(w,yy);ctx.stroke();ctx.fillText(String(p),3,yy-3)}
for(let z of srVisible){let px=x(z.known_column),py=y((Number(z.lower)+Number(z.upper))/2),left=Math.max(45,px),right=Math.min(w,x(Math.min(cols.length-1,z.known_column+20)));let color=z.type==='SUPPORT'?'#e6bd55':'#ac90ff';
if(right>=left){ctx.fillStyle=z.type==='SUPPORT'?'#e6bd552a':'#ac90ff2a';ctx.fillRect(left,y(Number(z.upper))-3,Math.max(2,right-left),Math.max(7,y(Number(z.lower))-y(Number(z.upper))+6));ctx.setLineDash([3,5]);ctx.strokeStyle=color;ctx.strokeRect(left,y(Number(z.upper))-3,Math.max(2,right-left),Math.max(7,y(Number(z.lower))-y(Number(z.upper))+6));ctx.setLineDash([])}
for(let col of [z.first_column,z.second_column]){let xx=x(col);if(xx>=45&&xx<w){ctx.beginPath();ctx.arc(xx,py,4,0,2*Math.PI);ctx.fillStyle=color;ctx.fill()}}if(px>=45&&px<w){ctx.fillStyle=color;ctx.font='bold 12px system-ui';ctx.fillText(z.type==='SUPPORT'?'S':'R',px+5,py-8)}}
for(let z of relevant){let termination=z.events.find(e=>e.type==='ZONE_INVALIDATED'||e.type==='ZONE_EXPIRED');let last=termination?colAt(termination.at):Math.max(z.column,colAt(z.at));let x1=x(z.column)-unit/2,x2=x(Math.max(z.column,last))+unit/2;let l=Math.max(45,x1),r=Math.min(w,x2);if(r>=l){ctx.fillStyle=z.direction==='LONG'?'#2cc29a44':'#ee827844';ctx.fillRect(l,y(Number(z.upper)),Math.max(2,r-l),Math.max(3,y(Number(z.lower))-y(Number(z.upper))));ctx.setLineDash([4,4]);ctx.strokeStyle=z.direction==='LONG'?'#35d7ad':'#ff9e8e';ctx.strokeRect(l,y(Number(z.upper)),Math.max(2,r-l),Math.max(3,y(Number(z.lower))-y(Number(z.upper))));ctx.setLineDash([])}
let pts=z.pivots.map(p=>[x(p.column),y(Number(p.price))]);ctx.strokeStyle=z.direction==='LONG'?'#4fd6ac':'#fa9f93';ctx.lineWidth=1.5;ctx.beginPath();pts.forEach(([px,py],j)=>j?ctx.lineTo(px,py):ctx.moveTo(px,py));ctx.stroke();ctx.font='bold 12px system-ui';pts.forEach(([px,py],j)=>{if(px>=47&&px<w){ctx.fillStyle=ctx.strokeStyle;ctx.fillText('XABC'[j],px-4,py-8)}});
for(let e of z.events){let px=x(colAt(e.at)),py=y((Number(z.lower)+Number(z.upper))/2);if(px<47||px>w)continue;ctx.fillStyle=e.type==='ZONE_FULL_TEST'?'#fff1a6':e.type==='ZONE_CONTACT'?'#fff':'#fdba85';ctx.beginPath();ctx.moveTo(px,py-6);ctx.lineTo(px+6,py);ctx.lineTo(px,py+6);ctx.lineTo(px-6,py);ctx.fill()}}
ctx.font=`${Math.max(12,unit*.72)}px monospace`;ctx.textAlign='center';for(let i=start;i<end;i++){let c=cols[i],px=x(i);ctx.fillStyle=c.kind==='X'?'#7fa9ff':'#ef91a1';let n=Math.min(200,Math.round((c.top-c.bottom)/box)+1);for(let k=0;k<n;k++){let p=c.bottom+k*box,yy=y(p);if(yy>top&&yy<bottom)ctx.fillText(c.kind==='X'?'×':'○',px,yy+5)}if(i%Math.max(1,Math.ceil(140/unit))===0){ctx.fillStyle='#a9bed8';ctx.font='11px system-ui';ctx.fillText(new Date(c.start).toISOString().slice(0,10),px,h-13);ctx.font=`${Math.max(12,unit*.72)}px monospace`}}ctx.textAlign='left';ctx.fillStyle='#c6d6e8';ctx.font='12px system-ui';ctx.fillText(`στήλες ${start}–${end-1} · τιμές ${low}–${high}`,55,18)}
document.getElementById('prev').onclick=()=>select(selected-1);document.getElementById('next').onclick=()=>select(selected+1);document.getElementById('all').onclick=()=>{view.scrollLeft=0;draw()};pick.onchange=()=>select(Number(pick.value));zoom.oninput=()=>{let before=view.scrollLeft/unit;unit=Number(zoom.value);strip.style.width=cols.length*unit+'px';view.scrollLeft=before*unit;draw()};
month.onchange=()=>{let ts=Date.UTC(2024,Number(month.value)-1,1);view.scrollLeft=colAt(ts)*unit;draw()};
for(let id of ['showLong','showShort'])document.getElementById(id).onchange=draw;
document.getElementById('showSr').onchange=draw;pickSr.onchange=()=>{if(pickSr.value!=='')selectSr(Number(pickSr.value))};
document.getElementById('prevSr').onclick=()=>selectSr(selectedSr<0?0:selectedSr-1);document.getElementById('nextSr').onclick=()=>selectSr(selectedSr+1);
strip.style.width=cols.length*unit+'px';view.addEventListener('scroll',()=>requestAnimationFrame(draw));window.addEventListener('resize',draw);showDetail();draw();
</script></html>'''
    return (template.replace("DATA_JSON", data)
            .replace("SOURCE_HASH", html.escape(source_hash))
            .replace("REPORT_HASH", html.escape(report_hash)))


def run(report_path: Path, candles: Path, output: Path) -> dict:
    if output.exists() or not output.parent.is_dir() or not report_path.is_file() or not candles.is_file():
        raise ValueError("existing input files and new output required")
    report_hash = sha256(report_path)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    zones = extract_zones(report)
    columns, coarse_pivots = replay_multiscale(candles)
    pivots = [f for f in report["decision_facts"] if f.get("type") == "PIVOT_CONFIRMED"]
    for pivot in pivots:
        i = pivot["column_id"]
        if (type(i) is not int or not 0 <= i < len(columns)-1
                or pivot["confirmed_at"] != columns[i+1]["start"]
                or pivot["kind"] != ("HIGH" if columns[i]["kind"] == "X" else "LOW")
                or number(pivot["price"]) != number(str(columns[i]["top" if pivot["kind"] == "HIGH" else "bottom"]))):
            raise ValueError("report pivot differs from pinned P&F replay")
    structural = map_coarse_zones(columns, coarse_pivots)
    page = chart_html(columns, zones, structural_zones=structural,
                      source_hash=SOURCE_SHA256, report_hash=report_hash)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="\n", dir=output.parent,
                                         prefix=".prz_chart_", suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(page)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return {"output": str(output), "gartley_prz": len(zones),
            "structural_zones": len(structural), "pnf_columns": len(columns),
            "source_sha256": SOURCE_SHA256, "report_sha256": report_hash,
            "execution": "OFF"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--candles", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.report, args.candles, args.output), indent=2))


if __name__ == "__main__":
    main()
