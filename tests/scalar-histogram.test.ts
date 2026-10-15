import { createRequire } from "node:module";
import { expect,it } from "vitest";
const {Histogram,Registry}=createRequire(new URL("../apps/engine/package.json",import.meta.url))("prom-client");
import { ScalarHistogram } from "../apps/engine/src/scalar-histogram.js";
it("exports every bucket, count and exact sum identically to the installed prom-client histogram",async()=>{
  const a=new Registry(),b=new Registry();const buckets=[.001,.005,.01,.02,.05,.1,.25,.5,1,5];
  const reference=new Histogram({name:"exact_histogram",help:"same observations",buckets,registers:[a]});
  const fast=new ScalarHistogram({name:"exact_histogram",help:"same observations",buckets,registers:[b]});
  for(const value of [-1,0,...buckets,5.00001,100,...Array.from({length:10000},(_,i)=>(i*31%1007)/1000)]){reference.observe(value);fast.observe(value);}
  expect(await b.metrics()).toBe(await a.metrics());
  expect(await b.getMetricsAsJSON()).toEqual(await a.getMetricsAsJSON());
  for(const value of [NaN,Infinity,-Infinity])expect(()=>fast.observe(value)).toThrow();
  fast.reset();reference.reset();expect(await b.metrics()).toBe(await a.metrics());reference.observe(0);fast.observe(0);
  expect(await b.metrics()).toBe(await a.metrics());
});
