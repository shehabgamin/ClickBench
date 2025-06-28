#!/usr/bin/env python3

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import timeit
import psutil
import json
import sys
import re
import subprocess
import functools

spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()

df = (spark
      .read
      .option("BINARY_AS_STRING", "true")
      .option("PUSHDOWN_FILTERS", "true")
      .option("REORDER_FILTERS", "true")
      .parquet("hits.parquet"))
df.createOrReplaceTempView("hits")

with open("queries.sql") as f:
    sql_text = f.read()
queries = [q.strip() for q in sql_text.split(";") if q.strip()]

all_timings = []
for idx, query in enumerate(queries):
    subprocess.run("sync", shell=True)
    # subprocess.run("echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null", shell=True)

    # Replace \1 to $1 because spark recognizes only this pattern style (in query 28)
    query = re.sub(r"""(REGEXP_REPLACE\(.*?,\s*('[^']*')\s*,\s*)('1')""", r"\1'$1'", query)
    print(f"\nQuery {idx}:\n{query}\n")

    timings = []
    for try_num in range(3):
        try:
            start = timeit.default_timer()
            result = spark.sql(query)
            res = result.toPandas()
            end = timeit.default_timer()
            total_time = end - start
            timings.append(total_time)
            if try_num == 0:
                print(res)
            print("Time: ", total_time)
        except Exception as e:
            print(e)
            print("Failure!")
    all_timings.append(timings)

with open("sail.jsonl", "w") as f:
    for timings in all_timings:
        f.write(json.dumps(timings) + "\n")