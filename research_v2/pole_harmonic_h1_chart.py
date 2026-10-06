"""Self-contained offline H1 candle view of previously projected PRZ zones.

H1 OHLC is aggregated from pinned complete 1m candles. Zone decisions remain
at their original exact close timestamp. No new patterns, fills or P&L.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import tempfile
from pathlib import Path

from research_v2.du_plessis_poles_annual import (
    FIRST_CLOSE_MS, LAST_CLOSE_MS, SOURCE_MINUTES, SOURCE_SHA256,
)
from research_v2.gartley_pole_prz import number
from research_v2.pole_harmonic_overlap import sha256

HOUR_MS = 3600000


def aggregate_h1(path: Path, *, expected_minutes: int = SOURCE_MINUTES,
                 first_close: int = FIRST_CLOSE_MS,
                 last_close: int = LAST_CLOSE_MS) -> list[dict]:
    bars=[]
    active=None
    previous=None
    count=0
    with path.open(newline='',encoding='utf-8-sig') as stream:
        reader=csv.DictReader(stream)
        if not reader.fieldnames or not {'close_time','open','high','low','close'} <= set(reader.fieldnames):
            raise ValueError('1m OHLC columns required')
        for row in reader:
            ts=int(row['close_time'])
            if (previous is not None and ts != previous+60000) or ts % 60000 != 59999:
                raise ValueError('noncontiguous closed 1m candles')
            op, hi, lo, cl=(number(row[k]) for k in ('open','high','low','close'))
            if lo <= 0 or lo > min(op,cl) or hi < max(op,cl):
                raise ValueError('invalid OHLC')
            hour=(ts-59999)//HOUR_MS*HOUR_MS
            if active is None or active['at'] != hour:
                if active is not None:
                    if active.pop('minutes') != 60:
                        raise ValueError('incomplete H1 candle')
                    bars.append({k:str(v) if k!='at' else v for k,v in active.items()})
                active={'at':hour,'open':op,'high':hi,'low':lo,'close':cl,'minutes':0}
            else:
                active['high']=max(active['high'],hi)
                active['low']=min(active['low'],lo)
                active['close']=cl
            active['minutes']+=1
            previous=ts
            count+=1
            if count>expected_minutes:
                raise ValueError('excess candles')
    if active is None or active.pop('minutes') != 60:
        raise ValueError('incomplete last H1 candle')
    bars.append({k:str(v) if k!='at' else v for k,v in active.items()})
    if (count, bars[0]['at']+59999,previous) != (expected_minutes, first_close,last_close):
        raise ValueError('incomplete pinned 1m period')
    return bars


def _dataset_from_html(page: str) -> dict:
    marker='<script id="dataset" type="application/json">'
    if page.count(marker)!=1 or page.count('</script>',page.index(marker))<1:
        raise ValueError('invalid source chart')
    return json.loads(page.split(marker,1)[1].split('</script>',1)[0])


def make_page(bars: list[dict], zones: list[dict], observations: list[dict],
              source_hash: str, chart_hash: str, overlap_hash: str) -> str:
    if not bars or not zones:
        raise ValueError('H1 bars and zones required')
    ids=set()
    for z in zones:
        at=z.get('known_at')
        lo,hi=number(z.get('lower')),number(z.get('upper'))
        if (z.get('type')!='NONCONSECUTIVE_GARTLEY_PROJECTION'
                or z.get('direction') not in ('LONG','SHORT')
                or type(at) is not int or at % 60000 != 59999
                or at < bars[0]['at'] or at >= bars[-1]['at']+HOUR_MS
                or lo<=0 or hi<lo or hi-lo>1000 or not isinstance(z.get('id'),str)
                or z['id'] in ids):
            raise ValueError('invalid projected zone')
        ids.add(z['id'])
    displayed=[]
    for obs in observations:
        if obs.get('category') not in ('IN_ZONE','NEAR_1_COARSE_BOX'):
            continue
        key=obs.get('nearest_zone_id')
        if key is None:
            continue
        if key not in ids or type(obs.get('signal_ts')) is not int:
            raise ValueError('invalid existing pole association')
        z=next(z for z in zones if z['id']==key)
        if obs['signal_ts'] <= z['known_at'] or obs.get('direction')!=z['direction']:
            raise ValueError('pole precedes projection')
        displayed.append({'id':obs['event_id'],'zone_id':key,'at':obs['signal_ts'],
                          'close':str(number(obs['signal_close'])),
                          'category':obs['category']})
    data=json.dumps({'bars':bars,'zones':zones,'signals':displayed},
                    separators=(',',':'),ensure_ascii=True).replace('<','\\u003c')
    template=r'''<!doctype html><html lang="el"><meta charset="utf-8"><title>BTCUSDT 2024 · H1 και ιστορικές PRZ</title>
<style>body{margin:0;background:#101b2a;color:#eaf3ff;font:14px system-ui,Segoe UI,sans-serif}header{position:sticky;top:0;z-index:2;background:#1b2c43;padding:12px 18px}h1{font-size:20px;margin:0 0 8px}button,select,input{background:#304866;color:#fff;border:1px solid #7190ad;border-radius:4px;padding:6px}#viewport{overflow-x:auto;border-block:1px solid #5c7999}#strip{height:650px}canvas{position:sticky;left:0;display:block}#detail{padding:12px 18px;line-height:1.5}.muted{color:#b8c9d9}</style>
<header><h1>BTCUSDT · κεριά H1 και οι 5 ερευνητικές προβολές Fibonacci</h1>
<button id="prev">◀ Ζώνη</button> <select id="pick" aria-label="Επιλογή ζώνης"></select> <button id="next">Ζώνη ▶</button>
<label>Μεγέθυνση <input id="zoom" type="range" min="5" max="18" value="9"></label></header>
<div id="viewport"><div id="strip"><canvas id="canvas" aria-label="Κυλιόμενος ωριαίος χάρτης"></canvas></div></div>
<div id="detail"></div><div id="note" class="muted" style="padding:10px 18px">H1 από 60 πλήρη κλεισμένα κεριά 1m ανά ώρα, UTC. Το mixed confirmation hour περιέχει λεπτά πριν και μετά την επιβεβαίωση: ΔΕΝ ερμηνεύεται ως επαφή μετά τη δημιουργία. Η σκίαση μετά τη γραμμή επιβεβαίωσης είναι μόνο προβολή έως 60 ημέρες, ΟΧΙ διάρκεια ισχύος. Τα ◆ είναι υπάρχοντα pole signals, όχι fills. Έρευνα μόνο · χωρίς νέο backtest, fees ή εντολές.<br>1m SHA-256: SOURCE_HASH · chart SHA-256: CHART_HASH · overlap SHA-256: OVERLAP_HASH</div>
<script id="dataset" type="application/json">DATA_JSON</script><script>
'use strict';const {bars,zones,signals}=JSON.parse(document.getElementById('dataset').textContent),hour=3600000;
const view=document.getElementById('viewport'),strip=document.getElementById('strip'),canvas=document.getElementById('canvas'),ctx=canvas.getContext('2d'),pick=document.getElementById('pick'),detail=document.getElementById('detail'),zoom=document.getElementById('zoom');let selected=0,unit=9,windowBars=[];
function utc(t){return new Date(t).toISOString().replace('T',' ').slice(0,16)+' UTC'}
function choose(i){selected=(i+zones.length)%zones.length;pick.value=selected;let z=zones[selected],begin=z.known_at-72*hour,end=z.known_at+60*24*hour;windowBars=bars.filter(b=>b.at+hour>begin&&b.at<end);strip.style.width=windowBars.length*unit+'px';let at=windowBars.findIndex(b=>b.at+hour>z.known_at);view.scrollLeft=Math.max(0,(at-8)*unit);let relevant=signals.filter(s=>s.zone_id===z.id);detail.textContent=`${selected+1}/${zones.length} · ${z.direction} ${z.lower}–${z.upper} · γνωστή ${utc(z.known_at)} · αδρές στήλες ${(z.coarse_pivot_columns||[]).join('/')} · ${relevant.length} συνδεδεμένα ιστορικά pole signals · η ισχύς της ζώνης δεν έχει ελεγχθεί`;draw()}
for(let i=0;i<zones.length;i++){let z=zones[i],o=document.createElement('option');o.value=i;o.textContent=`${i+1}. ${utc(z.known_at)} · ${z.direction} · ${z.lower}–${z.upper}`;pick.append(o)}
function draw(){let w=view.clientWidth,h=650,dpr=window.devicePixelRatio||1;canvas.width=Math.round(w*dpr);canvas.height=Math.round(h*dpr);canvas.style.width=w+'px';canvas.style.height=h+'px';ctx.setTransform(dpr,0,0,dpr,0,0);ctx.fillStyle='#101b2a';ctx.fillRect(0,0,w,h);if(!windowBars.length)return;
let left=view.scrollLeft,start=Math.max(0,Math.floor(left/unit)-2),end=Math.min(windowBars.length,Math.ceil((left+w)/unit)+2),z=zones[selected],seen=windowBars.slice(start,end);if(!seen.length)return;
let values=seen.flatMap(b=>[Number(b.high),Number(b.low)]);values.push(Number(z.lower),Number(z.upper));let lo=Math.min(...values),hi=Math.max(...values),pad=Math.max(1,(hi-lo)*.08);lo-=pad;hi+=pad;let y=p=>h-45-(p-lo)/(hi-lo)*(h-78),x=i=>i*unit-left+unit/2;
ctx.strokeStyle='#32465e';ctx.fillStyle='#b4c8df';ctx.font='11px sans-serif';for(let j=0;j<=8;j++){let p=lo+(hi-lo)*j/8,yy=y(p);ctx.beginPath();ctx.moveTo(65,yy);ctx.lineTo(w,yy);ctx.stroke();ctx.fillText(p.toFixed(0),5,yy-2)}
let exact=windowBars.findIndex(b=>b.at+hour>z.known_at),knownX=x(exact)+(z.known_at-windowBars[exact].at)/hour*unit-unit/2,first=Math.max(65,knownX),last=Math.min(w,knownX+60*24*unit);
if(last>first){ctx.fillStyle='#56d9fa25';ctx.fillRect(first,y(Number(z.upper)),last-first,Math.max(2,y(Number(z.lower))-y(Number(z.upper))))}
if(knownX>=65&&knownX<w){ctx.strokeStyle='#f9d36a';ctx.setLineDash([5,4]);ctx.beginPath();ctx.moveTo(knownX,25);ctx.lineTo(knownX,h-45);ctx.stroke();ctx.setLineDash([]);ctx.fillStyle='#f9d36a';ctx.fillText('PRZ γνωστή',knownX+4,32)}
for(let i=start;i<end;i++){let b=windowBars[i],px=x(i),up=Number(b.close)>=Number(b.open);ctx.strokeStyle=up?'#43d9ae':'#f18494';ctx.fillStyle=up?'#43d9ae':'#f18494';ctx.beginPath();ctx.moveTo(px,y(Number(b.high)));ctx.lineTo(px,y(Number(b.low)));ctx.stroke();let a=y(Number(b.open)),c=y(Number(b.close));ctx.fillRect(px-Math.max(1,unit*.28),Math.min(a,c),Math.max(2,unit*.56),Math.max(2,Math.abs(a-c)));if(i%Math.max(1,Math.ceil(110/unit))===0){ctx.fillStyle='#b4c8df';ctx.fillText(utc(b.at).slice(0,10),px,h-16)}}
for(let s of signals){if(s.zone_id!==z.id||s.at<windowBars[start].at||s.at>=windowBars[end-1].at+hour)continue;let idx=Math.floor((s.at-windowBars[0].at)/hour),px=x(idx),py=y(Number(s.close));ctx.fillStyle='#fff4a8';ctx.beginPath();ctx.moveTo(px,py-6);ctx.lineTo(px+6,py);ctx.lineTo(px,py+6);ctx.lineTo(px-6,py);ctx.fill()}
ctx.fillStyle='#dae8f9';ctx.font='12px sans-serif';ctx.fillText(`${utc(seen[0].at)} – ${utc(seen.at(-1).at)} · ${z.lower}–${z.upper}`,70,16)}
document.getElementById('prev').onclick=()=>choose(selected-1);document.getElementById('next').onclick=()=>choose(selected+1);pick.onchange=()=>choose(Number(pick.value));zoom.oninput=()=>{let position=view.scrollLeft/unit;unit=Number(zoom.value);strip.style.width=windowBars.length*unit+'px';view.scrollLeft=position*unit;draw()};view.addEventListener('scroll',()=>requestAnimationFrame(draw));window.addEventListener('resize',draw);choose(0);
</script></html>'''
    return (template.replace('DATA_JSON',data).replace('SOURCE_HASH',source_hash)
            .replace('CHART_HASH',chart_hash).replace('OVERLAP_HASH',overlap_hash))


def run(candles: Path, chart: Path, overlap: Path, output: Path) -> dict:
    if (output.exists() or not output.parent.is_dir()
            or not all(p.is_file() for p in (candles,chart,overlap))):
        raise ValueError('existing inputs and new output required')
    if sha256(candles)!=SOURCE_SHA256:
        raise ValueError('pinned 1m SHA-256 mismatch')
    chart_hash,overlap_hash=sha256(chart),sha256(overlap)
    source=_dataset_from_html(chart.read_text(encoding='utf-8'))
    context=json.loads(overlap.read_text(encoding='utf-8'))
    zones=source['harmonic']
    if (context.get('schema')!='pole-harmonic-overlap-descriptive-v1'
            or context.get('execution')!='OFF'
            or context.get('source_sha256')!=SOURCE_SHA256
            or context.get('chart_sha256')!=chart_hash
            or context.get('harmonic_zones')!=len(zones)):
        raise ValueError('incompatible chart/overlap provenance')
    bars=aggregate_h1(candles,expected_minutes=SOURCE_MINUTES,
                      first_close=FIRST_CLOSE_MS,last_close=LAST_CLOSE_MS)
    if sha256(candles)!=SOURCE_SHA256:
        raise ValueError('changed source candles')
    page=make_page(bars,zones,context['observations'],SOURCE_SHA256,chart_hash,overlap_hash)
    temporary=None
    try:
        with tempfile.NamedTemporaryFile('w',encoding='utf-8',newline='\n',dir=output.parent,
                                         prefix='.prz_h1_',suffix='.tmp',delete=False) as stream:
            temporary=Path(stream.name)
            stream.write(page)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary,output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return {'output':str(output),'source_sha256':SOURCE_SHA256,'chart_sha256':chart_hash,
            'overlap_sha256':overlap_hash,'h1_bars':len(bars),'zones':len(zones),
            'close_or_near_signals':sum(o['category'] in ('IN_ZONE','NEAR_1_COARSE_BOX')
                                        for o in context['observations']),
            'execution':'OFF'}


def main() -> None:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candles',type=Path,required=True)
    parser.add_argument('--chart',type=Path,required=True)
    parser.add_argument('--overlap',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    print(json.dumps(run(args.candles,args.chart,args.overlap,args.output),indent=2))


if __name__=='__main__':
    main()
