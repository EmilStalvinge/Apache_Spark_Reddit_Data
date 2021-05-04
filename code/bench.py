#!/usr/bin/env python3
#
# Benchmarks subreddit analysis algorithms on a spark cluster
#
# Usage: bench <benchmark> [<year> [<cores> [<limit>]]]
# where
#   <benchmark> is the benchmark, "count" or "top10".
#   <year> is the year or "all". Default: "all".
#   <cores> is the number of cores. Default: all cores.
#   <limit> is the maximum number of rows. Default: all rows.
#
# Examples:
#   Show the top 10 over all years, using all cores and rows:
#       bench top10
#   Count the number of rows for year 2009:
#       bench count 2009
#   Show the top 10 for year 2011, using 4 cores:
#       bench top10 2011 4
#   Show the top 10 over all years, using 1 core and limit the number of
#   rows to 1000000:
#       bench top10 all 1 1000000

import sys, timeit
from pyspark.sql import SparkSession

def rowcount(df):
    rows = df.count()
    print("Total number of of comments:", rows)

def top10(df):
    print("Top 10 subreddits:")
    df.groupBy("subreddit")\
        .count()\
        .orderBy("count",ascending=False)\
        .show(10)

if __name__ == "__main__":
    year = ""
    cores = 8
    limit = None

    if len(sys.argv) == 1 or sys.argv[1] == '--help':
        print("Usage: bench benchmark [<year> [<cores> [<limit>]]]")
        sys.exit()
    if sys.argv[1] == 'count':
        benchfunc = rowcount
    elif sys.argv[1] == 'top10':
        benchfunc = top10
    else:
        print("Unknown benchmark.")
        sys.exit()
    if len(sys.argv) > 2:
        year = sys.argv[2] if sys.argv[2] != "all" else ""
    if len(sys.argv) > 3:
        cores = int(sys.argv[3])
    if len(sys.argv) > 4:
        limit = int(sys.argv[4])

    rcfilenames = 'hdfs://192.168.2.237:9000/reddit/rc-' + year + '*.csv.bz2'

    #spark = SparkSession\
    #    .builder\
    #    .master("spark://spark-master:7077")\
    #    .appName("bench")\
    #    .config("spark.dynamicAllocation.enabled", True)\
    #    .config("spark.shuffle.service.enabled", True)\
    #    .config("spark.dynamicAllocation.executorIdleTimeout","30s")\
    #    .config("spark.executor.cores",2)\
    #    .config("spark.cores.max",cores)\
    #    .getOrCreate()

    spark = SparkSession\
        .builder\
        .master("spark://spark-master:7077")\
        .appName("bench")\
        .config("spark.cores.max",cores)\
        .getOrCreate()

    df = spark.read\
        .option("header", "true")\
        .csv(rcfilenames,sep=', ')\
        .toDF('date', 'subreddit')\
        .cache()

    if limit:
        df = df.limit(limit)

    print("--- First run ---")
    t0 = timeit.default_timer()
    benchfunc(df)
    t1 = timeit.default_timer()
    print(f'Time to compute: {t1-t0:.2f} seconds.')

    print("\n--- Second run ---")
    t0 = timeit.default_timer()
    benchfunc(df)
    t1 = timeit.default_timer()
    print(f'Time to compute: {t1-t0:.2f} seconds.')

    spark.stop()
