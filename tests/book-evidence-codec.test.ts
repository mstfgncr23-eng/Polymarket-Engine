import {it,expect} from 'vitest';
import {BookEvidenceEncoder,BookEvidenceDecoder} from '../apps/engine/src/book-evidence-codec.js';
import {snapshot} from './fixtures.js';
it('losslessly encodes actual headers, level insert/update/delete and ordering without filling gaps',()=>{
  const encoder=new BookEvidenceEncoder(),decoder=new BookEvidenceDecoder();
  const first=snapshot();
  const snapshots=[first,{...first,eventId:'b2',sequence:2,bids:[{price:'0.46',quantity:'2'},...first.bids],asks:[{price:'0.48',quantity:'1'}]},
    {...first,eventId:'b3',sequence:3,bids:[],asks:[{price:'0.51',quantity:'8'},{price:'0.50',quantity:'9'}],wsCallbackAt:first.receivedAt},
    {...first,eventId:'b4',sequence:4,hash:'new',feeSchedule:null}];
  for(const book of snapshots){const encoded=encoder.encode(book,1,1,true);expect(decoder.apply(JSON.parse(JSON.stringify(encoded)))).toEqual(book);}
  const minute=encoder.encode({...first,eventId:'minute'},2,1,true);expect(minute.encoding).toBe('FULL');
  expect(encoder.encode({...first,eventId:'epoch'},2,2,true).encoding).toBe('FULL');
  expect(encoder.encode({...first,eventId:'gap'},2,2,false).encoding).toBe('FULL');
  encoder.forget(first.tokenId);expect(encoder.encode(first,2,2,true).encoding).toBe('FULL');
  encoder.clear();expect(encoder.encode(first,2,2,true).encoding).toBe('FULL');
  expect(()=>new BookEvidenceDecoder().apply({encoding:'DELTA',snapshot:{...first,eventId:'orphan'},baseEventId:'absent',bidsDelta:[],asksDelta:[]})).toThrow('BOOK_EVIDENCE_BASE_MISSING');
});
it('retains its own depth when the adapter reuses an array',()=>{
  const encoder=new BookEvidenceEncoder(),decoder=new BookEvidenceDecoder(),book=snapshot();
  decoder.apply(encoder.encode(book,1,1,true));
  book.bids[0]!.quantity='999';book.eventId='changed';
  expect(decoder.apply(encoder.encode(book,1,1,true))).toEqual(book);
});
it('retains headers losslessly across changes, deletions and mutable fee schedules',()=>{
  const encoder=new BookEvidenceEncoder(),decoder=new BookEvidenceDecoder(),book=snapshot();
  decoder.apply(encoder.encode(book,1,1,true));
  book.eventId='new-header';delete book.wsCallbackAt;
  if(book.feeSchedule)book.feeSchedule.rate='0.03';
  const encoded=encoder.encode(book,1,1,true);
  expect(encoded.encoding).toBe('DELTA_V2');
  expect(decoder.apply(JSON.parse(JSON.stringify(encoded)))).toEqual(book);
  const next={...book,eventId:'next',feeSchedule:null};
  expect(decoder.apply(JSON.parse(JSON.stringify(encoder.encode(next,1,1,true))))).toEqual(next);
});
