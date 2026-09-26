# bench/spike/curve.py  (Appendix A of the design spike, committed as-is except for the output path)
"""Spike: growth curve at 60 KiB of new tool output per node (non-repeating text).
Columns: history payload bytes inline (no External Storage), history payload bytes
with External Storage at 256 KiB threshold, whole-blob store bytes, CDC-dedup store bytes."""
import asyncio, base64, dataclasses, hashlib, json, operator, random
from typing import Annotated
from typing_extensions import TypedDict
from langgraph.graph import StateGraph, START, END
from temporalio.converter import DataConverter, ExternalStorage, StorageDriver, StorageDriverClaim
from temporalio.contrib.langgraph._activity import ActivityInput, ActivityOutput
import fastcdc
MB = 1024*1024
class Mem(StorageDriver):
    def name(self): return "mem"
    async def store(self, ctx, ps): return [StorageDriverClaim(claim_data={"k": hashlib.sha256(p.SerializeToString()).hexdigest()}) for p in ps]
    async def retrieve(self, ctx, cs): raise NotImplementedError
class State(TypedDict):
    messages: Annotated[list, operator.add]
    findings: Annotated[list, operator.add]
    target: str
def run(n, kb, seed=7):
    rnd = random.Random(seed); cap = []
    g = StateGraph(State); prev = START
    for i in range(n):
        content = base64.b64encode(rnd.randbytes(kb*768)).decode()
        def node(state, i=i, content=content):
            d = {"messages": [{"role": "tool", "name": f"tool_{i}", "content": content}], "findings": [f"f{i}"]}
            cap.append((state, d)); return d
        g.add_node(f"n{i}", node); g.add_edge(prev, f"n{i}"); prev = f"n{i}"
    g.add_edge(prev, END); g.compile().invoke({"messages": [], "findings": [], "target": "acct"}); return cap
async def main():
    ext = dataclasses.replace(DataConverter.default, external_storage=ExternalStorage(drivers=[Mem()], payload_size_threshold=256*1024))
    cfg = {"tags": [], "metadata": {}, "configurable": {}, "context": None, "previous": None, "execution_info": None}
    out = []
    for n in [10, 20, 30, 40, 50, 60]:
        inline = ext_hist = whole = 0; seen = set(); dedup = 0; max_in = 0
        for st, d in run(n, 60):
            for val in (ActivityInput(args=(st,), kwargs={}, langgraph_config=cfg), ActivityOutput(result=d)):
                raw = (await DataConverter.default.encode([val]))[0]; rb = raw.SerializeToString()
                inline += len(rb)
                e = (await ext.encode([val]))[0]; ext_hist += e.ByteSize()
                if e.ByteSize() < raw.ByteSize():
                    whole += len(rb)
                    for c in fastcdc.fastcdc(rb, min_size=4096, avg_size=16384, max_size=65536, fat=True):
                        h = hashlib.sha256(c.data).digest()
                        if h not in seen: seen.add(h); dedup += c.length
                if isinstance(val, ActivityInput): max_in = max(max_in, len(rb))
        out.append(dict(nodes=n, largest_input_mb=round(max_in/MB,2), history_inline_mb=round(inline/MB,1),
                        history_with_extstore_mb=round(ext_hist/MB,2), store_whole_blob_mb=round(whole/MB,1), store_dedup_mb=round(dedup/MB,2)))
    print(json.dumps(out, indent=1))
    json.dump(out, open(__import__("pathlib").Path(__file__).with_name("curve.json"), "w"), indent=1)
asyncio.run(main())
