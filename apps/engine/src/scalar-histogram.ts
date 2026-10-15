import type { Histogram, Registry } from "prom-client";

/** Fixed-bucket, label-free Prometheus histogram. Records EVERY observation
 * with the same bucket boundaries and ordered IEEE sum as prom-client. The hot
 * path allocates no label object, closure or hash; export uses public Registry
 * metric/get protocol. No private prom-client fields are accessed. */
export class ScalarHistogram {
  readonly labelNames: string[] = [];
  readonly type = "histogram";
  readonly aggregator = "sum";
  readonly name: string;
  readonly help: string;
  private readonly bounds: readonly number[];
  private readonly bins: Float64Array;
  private count = 0;
  private sum = 0;
  private active = true;
  constructor(config: { name:string; help:string; buckets:readonly number[]; registers:Registry[] }) {
    this.name=config.name;this.help=config.help;this.bounds=[...config.buckets];
    if(!this.bounds.length||this.bounds.some((v,i)=>!Number.isFinite(v)||(i>0&&v<=this.bounds[i-1]!)))throw Error("INVALID_HISTOGRAM_BUCKETS");
    this.bins=new Float64Array(this.bounds.length);
    // prom-client's TypeScript union omits custom metrics; Registry's public
    // rendering protocol consumes name/help/type/aggregator/get, supplied here.
    for(const registry of config.registers)registry.registerMetric(this as unknown as Histogram<string>);
  }
  observe(value:number): void {
    if(!Number.isFinite(value))throw new TypeError("Value is not a valid number");
    this.active=true;
    this.sum+=value;this.count++;
    for(let i=0;i<this.bounds.length;i++)if(value<=this.bounds[i]!){this.bins[i]=this.bins[i]!+1;break;}
  }
  reset():void {this.count=0;this.sum=0;this.bins.fill(0);this.active=false;}
  async get() {
    let cumulative=0;
    const values:Array<{metricName:string;labels:Record<string,string|number>;value:number;exemplar:null|undefined}>=[];
    if(!this.active)return {name:this.name,help:this.help,type:this.type,aggregator:this.aggregator,values};
    for(let i=0;i<this.bounds.length;i++){
      cumulative+=this.bins[i]!;
      values.push({metricName:this.name+"_bucket",labels:{le:this.bounds[i]!},value:cumulative,exemplar:null});
    }
    values.push({metricName:this.name+"_bucket",labels:{le:"+Inf"},value:this.count,exemplar:null},
      {metricName:this.name+"_sum",labels:{},value:this.sum,exemplar:undefined},{metricName:this.name+"_count",labels:{},value:this.count,exemplar:undefined});
    return {name:this.name,help:this.help,type:this.type,aggregator:this.aggregator,values};
  }
}
