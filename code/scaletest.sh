#!/bin/bash
#
# Performs scalability testing
#
# Usage: scaletest <type> <year> [<limit>]
# where
#   <type> is either "weak" or "strong"
#   <year> is the year
#   <limit> is the maximum number of rows (comments). Default: all rows.
#           For weak testing, this is the limit per worker.
#
# Examples:
#   Peform a strong scalability on year 2007, all rows:
#       scaletest strong 2007
#   Perform a weak scalability testing on year 2009 with a limit of 1000000 rows:
#       scaletest weak 2009 1000000

WEAKORSTRONG=$1
YEAR=$2
ORIGLIMIT=$3
SLAVESFILE='/opt/spark/conf/slaves'
SLEEP=5
CORES=2

date
echo "Doing $WEAKORSTRONG scalability testing on year $YEAR with a limit of $ORIGLIMIT comments"
echo "----------------"
echo "Stopping cluster"
echo "----------------"
stop-all.sh
sleep $SLEEP
echo 'spark-master' > $SLAVESFILE
echo "-----------------------------------"
echo "Restarting cluster with master only"
echo "-----------------------------------"
start-all.sh
sleep $SLEEP
echo
echo "------------------"
echo "Counting benchmark"
echo "------------------"
bench count $YEAR $CORES $ORIGLIMIT
echo
echo "----------------"
echo "Top 10 benchmark"
echo "----------------"
bench top10 $YEAR $CORES $ORIGLIMIT

LIMIT=$ORIGLIMIT
for i in {1..3}; do
    let CORES=CORES+2 
    echo "----------------"
    echo "Stopping cluster"
    echo "----------------"
    stop-all.sh
    sleep $SLEEP
    echo "--------------------------------------------"
    echo "Adding spark-worker-$i and restarting cluster"
    if [ $WEAKORSTRONG == "weak" ]; then
        let LIMIT=LIMIT+ORIGLIMIT
        echo "Adjusting limit to $LIMIT comments"
    fi
    echo "--------------------------------------------"
    echo "spark-worker-$i" >> $SLAVESFILE
    start-all.sh
    sleep $SLEEP
    echo
    echo "------------------"
    echo "Counting benchmark"
    echo "------------------"
    bench count $YEAR $CORES $LIMIT
    echo
    echo "----------------"
    echo "Top 10 benchmark"
    echo "----------------"
    bench top10 $YEAR $CORES $LIMIT
done

date
