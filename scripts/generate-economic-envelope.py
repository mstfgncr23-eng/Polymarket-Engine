from decimal import Decimal as D,ROUND_HALF_UP
import json,pathlib
r=pathlib.Path(__file__).resolve().parents[1]
def fee(q,p):
 x=q*D('.07')*p*(1-p)
 return D(0) if x<D('.000005') else x.quantize(D('.00001'),rounding=ROUND_HALF_UP)
rows=[]
for price in ['.1','.2','.5','.65','.8','.9','.95','.97']:
 p=D(price);q=(D(5)/p).quantize(D('.00000001'))
 for spread in ['.01','.02','.08']:
  for score in ['.15','.25','.5','.75','1']:
   f=min(D('.99'),max(D('.01'),p-D(spread)/2+D('.05')*D(score)))
   modes={}
   for role in ['MAKER','TAKER']:
    entry=D(0) if role=='MAKER' else fee(q,p)
    for mode,exit in [('maximum',fee(q,D('.5'))),('fair_value',fee(q,f))]:
     modes[role+':'+mode]={'entryFee':str(entry),'exitFee':str(exit),'expectedROI':str((q*(f-p)-entry-exit)/((q*p).quantize(D('.00000001'),rounding=ROUND_HALF_UP) if role=='TAKER' else q*p))}
   rows.append({'price':str(p),'quantity':str(q),'fair':str(f),'modes':modes})
assert len(rows)==120
example=next(x for x in rows if x['price']=='0.95' and D(x['fair'])>D('.97'))
path=r/'tests/fixtures/economic-envelope-python.json';path.parent.mkdir(exist_ok=True);path.write_text(json.dumps(rows,indent=2));print('Independent Python Decimal cases generated:',len(rows))
