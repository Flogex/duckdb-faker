SHELL := /bin/bash

.PHONY: configure build debug release test format

ifneq (${OSX_BUILD_ARCH}, "")
	OSX_BUILD_FLAG=-DOSX_BUILD_ARCH=${OSX_BUILD_ARCH}
endif

GENERATOR=-G "Ninja" -DFORCE_COLORED_OUTPUT=1
EXTENSION_FLAGS=-DDUCKDB_EXTENSION_CONFIGS='${EXT_CONFIG}'
BUILD_FLAGS=$(GENERATOR) -DEXTENSION_STATIC_BUILD=1 $(EXTENSION_FLAGS) $(OSX_BUILD_FLAG) -DDUCKDB_EXPLICIT_PLATFORM='${DUCKDB_PLATFORM}'

default: release

configure:
ifeq ($(strip $(BUILD_TYPE)),)
	$(error BUILD_TYPE is not defined)
endif
	mkdir -p  build/${BUILD_TYPE}
	cmake $(BUILD_FLAGS) -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -S . -B build/${BUILD_TYPE}

build:
	@if [ ! -d "build/${BUILD_TYPE}" ] || [ ! -d "build/${BUILD_TYPE}/CMakeFiles" ]; then \
		$(MAKE) BUILD_TYPE=${BUILD_TYPE} configure; \
	fi
	cmake --build build/${BUILD_TYPE} --config ${BUILD_TYPE}

debug: BUILD_TYPE=Debug
debug: build

reldebinfo: BUILD_TYPE=RelWithDebInfo
reldebinfo: build

release: BUILD_TYPE=Release
release: build

test: BUILD_TYPE?=Debug
test: build
	./build/${BUILD_TYPE}/test/unittests

test_release: BUILD_TYPE=Release
test_release: build
	./build/${BUILD_TYPE}/test/unittests

format:
	@bash scripts/run-format.sh