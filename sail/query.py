#!/usr/bin/env python3

from pyspark.sql import SparkSession

import timeit
import sys
import re

query = sys.stdin.read()
# Replace \1 to $1 because spark recognizes only this pattern style (in query 28)
query = re.sub(r"""(REGEXP_REPLACE\(.*?,\s*('[^']*')\s*,\s*)('1')""", r"\1'$1'", query)
print(query)

spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()

df = (spark
      .read
      .option("BINARY_AS_STRING", "true")
      .option("PUSHDOWN_FILTERS", "true")
      .option("REORDER_FILTERS", "true")
      .parquet("hits.parquet"))
df.createOrReplaceTempView("hits")

for try_num in range(3):
    try:
        start = timeit.default_timer()
        result = spark.sql(query)
        res = result.toPandas()
        end = timeit.default_timer()
        total_time = end - start
        if try_num == 0:
            print(res)
        print("Time: ", total_time)
    except Exception as e:
        print(e)
        print("Failure!")

spark.stop()
