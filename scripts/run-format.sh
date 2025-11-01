#!/usr/bin/env bash

if [[ -z "${DISABLE_CLANG_FORMAT}" || "${DISABLE_CLANG_FORMAT}" == "0" ]]; then
  CURRENT_VERSION=$(clang-format --version | awk '{print $NF}'); \
  if [[ ! "${CURRENT_VERSION}" == 21* ]]; then
    echo "clang-format version 21 is required"
    exit 1
  fi

  SOURCE_FILES=$(find src test | grep -E ".*(\.cpp|\.hpp)$" | grep --invert-match "build/")
  if [[ -n "$SOURCE_FILES" ]]; then
    echo "Formatting C++ files:"
    echo "$SOURCE_FILES"
    clang-format -i -style=file "$SOURCE_FILES"
  fi
fi

if [[ -z "${DISABLE_CMAKE_FORMAT}" || "${DISABLE_CMAKE_FORMAT}" == "0" ]]; then
  CMAKE_FILES=$(find src test -name "CMakeLists.txt" -o -name "*.cmake" | grep --invert-match "build/")
  if [[ -n "$CMAKE_FILES" ]]; then
    echo "Formatting CMake files:"
    echo "$CMAKE_FILES"
    gersemi -i "$CMAKE_FILES"
  fi
fi

if [[ -z "${DISABLE_SHELL_FORMAT}" || "${DISABLE_SHELL_FORMAT}" == "0" ]]; then
  SHELL_FILES=$(find scripts -name "*.sh")
  if [[ -n "$SHELL_FILES" ]]; then
    echo "Formatting shell files:"
    echo "$SHELL_FILES"
    shfmt -i 2 -w "$SHELL_FILES"
  fi
  if [[ -n "$SHELL_FILES" ]]; then
    echo "Running linter for shell files:"
    echo "$SHELL_FILES"
    shellcheck "$SHELL_FILES"
  fi
fi