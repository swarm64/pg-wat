#!/bin/bash

set -e

IA_SQL_GH="https://raw.githubusercontent.com/swarm64/pg-wat/master/sql/analytics/impact_analyzer.sql"
IA_SQL_LOCAL="./sql/analytics/impact_analyzer.sql"
LOCAL=0

function help {
   echo "Usage: ./impact_analyzer.sh --dsn=<DSN to connect>"
   exit 0
}

function exit_if_not_set {
   if [ -z "$1" ]; then
      echo "Error: $2"
      help
      exit 1
   fi
}

function exit_if_not_found {
   if ! command -v $1 &> /dev/null; then
      echo "Error: $1 executable not found."
      exit 1
   fi
}

for arg in "$@"; do
case $arg in
   --dsn=*)
   DSN="${arg#*=}"
   shift
   ;;
   --local)
   LOCAL=1
   shift
   ;;
   --help)
   help
   shift
   ;;
   *)
   ;;
esac
done

exit_if_not_set "$DSN" "DSN not provided"
exit_if_not_found curl
exit_if_not_found psql


function run_impact_analyzer {
   psql $DSN -c "CALL s64da_impact_analyzer()" 2>&1
}

echo "Deploying impact analyzer SQL"
if [[ $LOCAL == 0 ]]; then
   curl $IA_SQL_GH | psql $DSN > /dev/null
else
   cat $IA_SQL_LOCAL | psql $DSN > /dev/null
fi

echo "Running impact analyzer on target DB"
exec 3< <(run_impact_analyzer)

tput clear
declare -Ag metrics
while read line <&3; do
   vars=`echo $line | sed 's/"//g' | grep -Po '([a-z_]+):([0-9:\.]+)'` || true
   if [[ -z $vars ]]; then
      continue
   fi

   while IFS=: read -r key value; do
      metrics[$key]=${value}
   done <<<$vars

   tput cup 0 0
   tput el
   tput bold
   printf "Duration: "

   tput sgr0
   printf "%s\n\n" ${metrics[duration]}

   tput bold
   printf "Queries\n"

   tput sgr0
   printf "Total:  %10d | Fast:   %10d | Medium: %10d | Slow:   %10d |\n" \
      ${metrics[num_queries]} ${metrics[fast]} ${metrics[medium]} ${metrics[slow]}

   printf "Tuples:            | INSERT: %10d | UPDATE: %10d | DELETE: %10d |\n" \
      ${metrics[inserts]} ${metrics[updates]} ${metrics[deletes]}

   tput bold
   printf "\nParallelism\n"

   tput sgr0
   printf "Min: %d | Max: %d | Avg: %d\n" \
      ${metrics[min_parallelism]} ${metrics[max_parallelism]} ${metrics[avg_parallelism]}

   tput bold
   printf "\nIO\n"

   tput sgr0
   printf "      |       Disk |      Cache | Ratio |\n"
   printf "Heap  | %10d | %10d |  %.2f |\n" \
      ${metrics[heap_disk]} ${metrics[heap_cache]} ${metrics[heap_ratio]}
   printf "Index | %10d | %10d |  %.2f |\n" \
      ${metrics[idx_disk]} ${metrics[idx_cache]} ${metrics[idx_ratio]}

   tput cup 0 0
done
