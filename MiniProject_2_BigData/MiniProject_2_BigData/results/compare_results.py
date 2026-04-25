import json, os

files = {
    'RDD': 'results/rdd_timings.json',
    'DataFrame': 'results/df_timings.json',
    'SQL': 'results/sql_timings.json'
}

print("\n" + "="*70)
print(f"{'PERFORMANCE COMPARISON TABLE':^70}")
print("="*70)
print(f"{'Query':<8} {'RDD (s)':<15} {'DataFrame (s)':<18} {'SQL (s)':<12} {'Best'}")
print("-"*70)

data = {}
for api, fpath in files.items():
    if os.path.exists(fpath):
        with open(fpath) as f:
            data[api] = json.load(f)
    else:
        data[api] = {}

for i in range(1, 11):
    qkey = f'Q{i}_time'
    rdd_t = data['RDD'].get(qkey, 'N/A')
    df_t  = data['DataFrame'].get(qkey, 'N/A')
    sql_t = data['SQL'].get(qkey, 'N/A')
    vals = {k: v for k, v in [('RDD', rdd_t), ('DF', df_t), ('SQL', sql_t)] if isinstance(v, float)}
    best = min(vals, key=vals.get) if vals else 'N/A'
    rdd_s  = f"{rdd_t:.3f}" if isinstance(rdd_t, float) else str(rdd_t)
    df_s   = f"{df_t:.3f}"  if isinstance(df_t, float) else str(df_t)
    sql_s  = f"{sql_t:.3f}" if isinstance(sql_t, float) else str(sql_t)
    print(f"Q{i:<7} {rdd_s:<15} {df_s:<18} {sql_s:<12} {best}")

print("="*70)
print("\nOptimization Results:")
if os.path.exists('results/optimization_timings.json'):
    with open('results/optimization_timings.json') as f:
        opt = json.load(f)
    for k, v in opt.items():
        print(f"  {k}: {v:.3f}s")
