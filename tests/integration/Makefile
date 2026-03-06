.PHONY: test build clean

ROOT_DIR := $(shell pwd)
RUST_EXT_DIR := $(ROOT_DIR)/rust_extension
GO_LOADER_DIR := $(ROOT_DIR)/go_loader
TARGET_DIR := $(RUST_EXT_DIR)/target/release
BIN_DIR := $(ROOT_DIR)/../../bin

# OS-specific extension suffix
ifeq ($(OS),Windows_NT)
    EXT_SUFFIX := .dll
else
    UNAME_S := $(shell uname -s)
    ifeq ($(UNAME_S),Darwin)
        EXT_SUFFIX := .dylib
    else
        EXT_SUFFIX := .so
    endif
endif

EXT_PATH := $(BIN_DIR)/libmyext$(EXT_SUFFIX)

test: build test-concurrency test-lazy

test-concurrency:
	@echo "Running Go concurrency & isolation stress test..."
	cd $(GO_LOADER_DIR) && EXT_PATH=$(EXT_PATH) $(BIN_DIR)/concurrency_test

test-lazy:
	@echo "Running Go dynamic/lazy loading test..."
	cd $(GO_LOADER_DIR) && EXT_PATH=$(EXT_PATH) $(BIN_DIR)/lazy_test

build:
	@mkdir -p $(BIN_DIR)
	@echo "Building Rust extension..."
	cd $(RUST_EXT_DIR) && cargo build --release
	@cp $(TARGET_DIR)/libintegration_ext$(EXT_SUFFIX) $(BIN_DIR)/libmyext$(EXT_SUFFIX)
	@echo "Building Go integration tests..."
	cd $(GO_LOADER_DIR) && go build -tags sqlite_extension -o $(BIN_DIR)/concurrency_test main.go
	cd $(GO_LOADER_DIR) && go build -tags sqlite_extension -o $(BIN_DIR)/lazy_test lazy_load.go

clean:
	cd $(RUST_EXT_DIR) && cargo clean
	rm -rf $(BIN_DIR)

leak-check: build
	@echo "Building C leak check program..."
	cd $(RUST_EXT_DIR) && \
		gcc leak_check.c -o $(BIN_DIR)/leak_check -lsqlite3
	@echo "Running leak check with Valgrind..."
	cd $(GO_LOADER_DIR) && EXT_PATH=$(EXT_PATH) \
	valgrind --leak-check=full --show-leak-kinds=all --errors-for-leak-kinds=definite $(BIN_DIR)/concurrency_test
