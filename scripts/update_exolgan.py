#!/usr/bin/env python3
"""Actualiza solamente escalas EXOLGAN. Requiere Python 3.11+."""
import json, pathlib, urllib.request, datetime, re
from html.parser import HTMLParser

ROOT=pathlib.Path(__file__).resolve().parents[1]
OUT=ROOT/'data'/'exolgan.json'
URL='https://transcargoarg.com/'
class Tables(HTMLParser):
    def __init__(self):
        super().__init__();self.tables=[];self.table=None;self.row=None;self.cell=None
    def handle_starttag(self,tag,attrs):
        if tag=='table': self.table=[]
        elif tag=='tr' and self.table is not None:self.row=[]
        elif tag in ('td','th') and self.row is not None:self.cell=[]
    def handle_data(self,data):
        if self.cell is not None:self.cell.append(data)
    def handle_endtag(self,tag):
        if tag in ('td','th') and self.cell is not None:
            self.row.append(' '.join(''.join(self.cell).split()));self.cell=None
        elif tag=='tr' and self.row is not None:
            if self.row:self.table.append(self.row)
            self.row=None
        elif tag=='table' and self.table is not None:
            self.tables.append(self.table);self.table=None
def main():
    req=urllib.request.Request(URL,headers={'User-Agent':'ARRIBOS/1.0 (schedule monitor; contact via repository)'})
    with urllib.request.urlopen(req,timeout=25) as resp:
        if resp.status!=200:raise RuntimeError('HTTP '+str(resp.status))
        html=resp.read(2_000_000).decode('utf-8','replace')
    parser=Tables();parser.feed(html)
    vessels={}
    for table in parser.tables:
        for row in table:
            if len(row)<5 or row[1].strip().upper()!='EXOLGAN':continue
            name,date,time,status=[row[i].strip() for i in (0,2,3,4)]
            if not re.fullmatch(r'\d{1,2}/\d{1,2}/\d{4}',date) or not re.fullmatch(r'\d{2}:\d{2}',time):continue
            eta=datetime.datetime.strptime(date+' '+time,'%d/%m/%Y %H:%M').strftime('%Y-%m-%dT%H:%M')
            key=name.upper()
            vessels[key]={'name':name,'eta':eta,'status':status,'terminal':'EXOLGAN'}
    if len(vessels)<5:raise RuntimeError('No se encontraron suficientes buques EXOLGAN; se conserva archivo anterior')
    old=json.loads(OUT.read_text()) if OUT.exists() else {}
    previous={v['name'].upper():v for v in old.get('vessels',[])}
    history=old.get('history',[])
    now=datetime.datetime.now(datetime.timezone.utc).isoformat()
    for key,new in vessels.items():
        prior=previous.get(key)
        if prior is None:history.append({'at':now,'ship':new['name'],'type':'nuevo','eta':new['eta']})
        else:
            if prior['eta']!=new['eta']:history.append({'at':now,'ship':new['name'],'type':'eta','before':prior['eta'],'after':new['eta']})
            if prior['status']!=new['status']:history.append({'at':now,'ship':new['name'],'type':'estado','before':prior['status'],'after':new['status']})
    # Una ausencia aislada no prueba cancelación: registrar desaparición para revisión.
    for key,prior in previous.items():
        if key not in vessels:history.append({'at':now,'ship':prior['name'],'type':'ausente_de_lista','before':prior.get('eta')})
    result={'updated_at':now,'source':URL,'timezone':'America/Argentina/Buenos_Aires','vessels':sorted(vessels.values(),key=lambda v:v['eta']),'history':history[-2000:]}
    OUT.parent.mkdir(exist_ok=True);OUT.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print('Exolgan:',len(vessels),'buques, eventos:',len(history))
if __name__=='__main__':main()
