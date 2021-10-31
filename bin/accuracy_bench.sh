#!/bin/bash
set +ex

JAVA="java"
JAR="./target/working-set-size-estimation-1.0-SNAPSHOT-jar-with-dependencies.jar"
CLASS_NAME="alluxio.client.file.cache.benchmark.BenchmarkMain"
BENCHMARK="accuracy"
DATASET="msr" # optional: msr, twitter, random, sequential
SHADOW_CACHE="ccf" # optional: ccf, mbf
TRACE="/datasets/msr-cambridge1/prxy_0.csv" # msr
#TRACE="/datasets/cluster37.0" # twitter
MAX_ENTRIES=12582912 # 12m
MEMORY="1mb"
WINDOW_SIZE=262144 # 256k
NUM_UNIQUE_ENTRIES=${WINDOW_SIZE} # used for random & sequential benchmark
CLOCK_BITS="4"
OPPO_AGING="true"
REPORT_DIR="/datasets/benchmarks/bench"
REPORT_INTERVAL=64

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
  local prefix="${REPORT_DIR}/${BENCHMARK}/${BENCHMARK}-${SHADOW_CACHE}-${DATASET}-${str_max_entries}-${str_window_size}-${MEMORY}-${CLOCK_BITS}-${OPPO_AGING}"
  REPORT_FILE="${prefix}.csv"
  LOG_FILE="${prefix}.log"
  echo "${REPORT_FILE}"
  echo "${LOG_FILE}"
  ${JAVA} -cp ${JAR} ${CLASS_NAME} \
    --benchmark ${BENCHMARK} \
    --shadow_cache ${SHADOW_CACHE} \
    --dataset ${DATASET} \
    --trace ${TRACE} \
    --max_entries ${MAX_ENTRIES} \
    --memory ${MEMORY} \
    --window_size ${WINDOW_SIZE} \
    --num_unique_entries ${NUM_UNIQUE_ENTRIES} \
    --clock_bits ${CLOCK_BITS} \
    --opportunistic_aging ${OPPO_AGING} \
    --report_file ${REPORT_FILE} \
    --report_interval ${REPORT_INTERVAL} \
    >> ${LOG_FILE}
}

MEMORY_LIST="1mb"
WINDOW_SIZE_LIST="262144" # 64k 256k 1m
CLOCK_BITS_LIST="4"
OPPO_AGING_LIST="true"

for MEMORY in ${MEMORY_LIST}; do
  for WINDOW_SIZE in ${WINDOW_SIZE_LIST}; do
    for CLOCK_BITS in ${CLOCK_BITS_LIST}; do
      for OPPO_AGING in ${OPPO_AGING_LIST}; do
        bench_one
      done
    done
  done
done
