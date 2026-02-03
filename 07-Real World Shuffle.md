# continuation ...

```
cluster setup:
 
1 master and 5 core nodes r7a 8X Large - 32 vCPU -> 160 vCPU
60-70 vCPU's after Spark overhead
aim is to get 256 MB
# partitions = total_shuffle_data/target_partition_size (should match spark.sql.shuffle.partitions)
3 TB/256 = 3072 GB * 4 ~ 12K partitions

typically we shoot for 2-4x shuffle partitions as the total # of exector cores
so with 70 vCPU's we can only have 280 shuffle partitions ~ 70 gig shuffle

So with only 5 core nodes, you have two options:

- Scale up nodes (very large instances)
- Preferably: Scale out using task nodes (more parallelism)
```

# AQE Overhead
https://chatgpt.com/c/69817670-0b4c-8331-800c-3756d0286d28
- Runs initial stages to collect real statistics (partition sizes, skew)
- Pauses at stage boundaries
- Replans:
  * splits skewed partitions
  * coalesces small partitions
  * may switch join strategies
- Downsides:
  * more shuffle metadata
  * extra tasks
  * planning/coordination overhead
  * ***not good*** for small data, latency critical micro-jobs, uniform keys

# Salting Overhead
- data modeling hack
- Code complexity ***long-term maintenance cost***
- Dimension side is replicated N times ***If you salt by 10 → dimension grows 10×***
- Many workflows require de-salting:  Extra aggregation stage + CPU + shuffle