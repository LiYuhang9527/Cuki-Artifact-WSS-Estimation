#!/bin/bash
set +ex

TARGET_DIR="G:\datasets\benchmarks\bench\accuracy"
PYTHON="python"
PLOTER="../src/main/python/plot_1.py"

plot_one() {
  ${PYTHON} ${PLOTER} "${1}"
}

CSV_FILES=$(ls "${TARGET_DIR}"/*.csv)

for FILE in ${CSV_FILES}; do
  echo -e "${FILE}"
  plot_one ${FILE}
  echo
done
