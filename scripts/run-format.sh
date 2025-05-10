#!/usr/bin/env bash

CURRENT_VERSION=$(clang-format --version | awk '{print $NF}'); \
if [[ ! "${CURRENT_VERSION}" == 20* ]]; then
  echo "clang-format version 20 is required"
  exit 1
fi

SOURCE_FILES=$(find src test | grep -E ".*(\.cpp|\.hpp)$" | grep --invert-match "build/")
echo "Formatting files:"
echo "$SOURCE_FILES"
clang-format -i -style=file $SOURCE_FILES