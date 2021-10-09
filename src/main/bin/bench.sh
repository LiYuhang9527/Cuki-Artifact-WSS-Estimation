#!/bin/bash
set +ex

JAVA="java"
JAR="./target/working-set-size-estimation-1.0-SNAPSHOT-jar-with-dependencies.jar"
CLASS_NAME="alluxio.client.file.cache.benchmark.ConcurrentBenchmark"
BENCHMARK="msr"
TRACE="/datasets/msr-cambridge1/prxy_0.csv" # msr
#TRACE="/datasets/cluster37.0" # twitter
MAX_ENTRIES=12582912 # 12m
MEMORY="1" # 1mb
WINDOW_SIZE=65536 # 64k
NUM_UNIQUE_ENTRIES=${MAX_ENTRIES} # used for random & sequential benchmark
CLOCK_BITS="2"
OPPO_AGING="true"
REPORT_DIR="/datasets/benchmarks/bench"
REPORT_INTERVAL=64
SIZE_ENCODING="BULKY"
SIZE_GROUP_BITS="4"
SIZE_BUCKET_BITS="4"

to_brief_string() {
  local size=$1
  if [[ ${size} -ge "1048576" ]]; then
    echo "$(( size / 1048576 ))m"
  elif [[ ${size} -ge "1024" ]]; then
    echo "$(( size / 1024 ))k"
  else
    echo "${size}k"
  fi
}

bench_one() {
  local str_max_entries=$(to_brief_string ${MAX_ENTRIES})
  local str_window_size=$(to_brief_string ${WINDOW_SIZE})
  mkdir -p "${REPORT_DIR}/${BENCHMARK}"
  local prefix="${REPORT_DIR}/${BENCHMARK}/${BENCHMARK}-${str_max_entries}-${str_window_size}-${MEMORY}mb-${CLOCK_BITS}-${OPPO_AGING}-${SIZE_ENCODING}-${SIZE_GROUP_BITS}-${SIZE_BUCKET_BITS}"
  REPORT_FILE="${prefix}.csv"
  LOG_FILE="${prefix}.log"
  echo "${REPORT_FILE}"
  ${JAVA} -cp ${JAR} ${CLASS_NAME} \
    -benchmark ${BENCHMARK} \
    -trace ${TRACE} \
    -max_entries ${MAX_ENTRIES} \
    -memory ${MEMORY} \
    -window_size ${WINDOW_SIZE} \
    -num_unique_entries ${NUM_UNIQUE_ENTRIES} \
    -clock_bits ${CLOCK_BITS} \
    -opportunistic_aging ${OPPO_AGING} \
    -report_file ${REPORT_FILE} \
    -report_interval ${REPORT_INTERVAL} \
    -size_encoding ${SIZE_ENCODING} \
    -size_group_bits ${SIZE_GROUP_BITS} \
    -size_bucket_bits ${SIZE_BUCKET_BITS} \
    >> ${LOG_FILE}
}

BENCHMARK_LIST="msr"
WINDOW_SIZE_LIST="65536 262144 1048576" # 64k 256k 1m
CLOCK_BITS_LIST="2 4 8"
OPPO_AGING_LIST="true false"
SIZE_ENCODING_LIST="BULKY GROUP AVG_GROUP LRU_GROUP"

for BENCHMARK in ${BENCHMARK_LIST}; do
  for WINDOW_SIZE in ${WINDOW_SIZE_LIST}; do
    for CLOCK_BITS in ${CLOCK_BITS_LIST}; do
      for OPPO_AGING in ${OPPO_AGING_LIST}; do
        for SIZE_ENCODING in ${SIZE_ENCODING_LIST}; do
          bench_one
        done
      done
    done
  done
done
