import { expect, it } from "vitest";
import { deriveBook, deriveBookState, type DerivedBookState } from "../packages/core/src/book-derived.js";
import { snapshot } from "./fixtures.js";
it("preserves exact Decimal-derived features across independent price/quantity/depth changes",()=>{
  let previous:DerivedBookState|undefined;
  const quantities=["0","1","9007199254740993","0.0000000000000000000000001","12345678901234567890.1234567890123456789"];
  for(let i=0;i<1500;i++){
    const n=i%23, m=(i*7)%29;
    const book=snapshot({bids:Array.from({length:n},(_,j)=>({price:(0.60-j*.001+(i%4)*.0001).toFixed(4),quantity:quantities[(i+j)%quantities.length]!})),
      asks:Array.from({length:m},(_,j)=>({price:(0.70+j*.001).toFixed(4),quantity:quantities[(i*3+j)%quantities.length]!}))});
    const next=deriveBookState(book,previous);
    expect(next.derived,`derived observation ${i}`).toEqual(deriveBook(book));
    previous=next;
  }
});
