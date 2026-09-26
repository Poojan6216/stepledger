# bench/spike/walls.py  (Appendix A of the design spike, committed as-is except for the output path)
"""Spike: which wall does a linear agent hit first? For each per-node output size,
the first node whose own Activity input exceeds 2 MiB, and the node count at which
cumulative history payload (inputs + outputs, no External Storage) crosses 10 MB and 50 MB.
Also history payload with External Storage at a 64 KiB threshold (reviewer check)."""
import asyncio, base64, dataclasses, hashlib, json, random
from temporalio.converter import DataConverter, ExternalStorage, StorageDriver, StorageDriverClaim
from temporalio.contrib.langgraph._activity import ActivityInput, ActivityOutput
MiB = 1024 * 1024; MB = 1024 * 1024
class Mem(StorageDriver):
    def name(self): return "mem"
    async def store(self, ctx, ps): return [StorageDriverClaim(claim_data={"k": hashlib.sha256(p.SerializeToString()).hexdigest()}) for p in ps]
    async def retrieve(self, ctx, cs): raise NotImplementedError
cfg = {"tags": [], "metadata": {}, "configurable": {}, "context": None, "previous": None, "execution_info": None}
async def walk(kb, max_nodes, ext=None):
    rnd = random.Random(7); msgs = []; findings = []; hist = 0; first_over = warn = limit = None; points = {}
    conv = ext or DataConverter.default
    for i in range(1, max_nodes + 1):
        state = {"messages": list(msgs), "findings": list(findings), "target": "acct"}
        content = base64.b64encode(rnd.randbytes(kb * 768)).decode()
        delta = {"messages": [{"role": "tool", "name": f"tool_{i}", "content": content}], "findings": [f"f{i}"]}
        inp = (await DataConverter.default.encode([ActivityInput(args=(state,), kwargs={}, langgraph_config=cfg)]))[0].ByteSize()
        if first_over is None and inp > 2 * MiB: first_over = i
        for v in (ActivityInput(args=(state,), kwargs={}, langgraph_config=cfg), ActivityOutput(result=delta)):
            hist += (await conv.encode([v]))[0].ByteSize()
        if warn is None and hist > 10 * MB: warn = i
        if limit is None and hist > 50 * MB: limit = i
        points[i] = round(hist / MB, 2)
        msgs += delta["messages"]; findings += delta["findings"]
        if ext is None and first_over and limit: break
    return first_over, warn, limit, points
async def main():
    rows = []
    for kb in (5, 10, 20, 40, 60, 100):
        fo, w, l, pts = await walk(kb, 400)
        rows.append({"kb_per_node": kb, "first_node_input_over_2mib": fo, "history_over_10mb_at_node": w,
                     "history_over_50mb_at_node": l, "first_wall": ("history 50 MB" if (l and (fo is None or l < fo)) else "single payload 2 MB")})
    _, _, _, p60 = await walk(60, 35)
    ext64 = dataclasses.replace(DataConverter.default, external_storage=ExternalStorage(drivers=[Mem()], payload_size_threshold=64 * 1024))
    _, _, _, pe = await walk(60, 60, ext=ext64)
    out = {"walls": rows, "history_inline_mb_at_35_nodes_60kib": p60[35],
           "history_ext64k_mb_60kib": {n: pe[n] for n in (10, 20, 30, 40, 50, 60)}}
    print(json.dumps(out, indent=1)); json.dump(out, open(__import__("pathlib").Path(__file__).with_name("walls.json"), "w"), indent=1)
asyncio.run(main())
