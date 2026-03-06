BIN_DIR := $(shell pwd)/bin

.PHONY: test coverage leak-check-valgrind leak-check-miri clean

test:
	cargo test

test-integration:
	$(MAKE) -C tests/integration test

leak-check-integration:
	@mkdir -p $(BIN_DIR)
	@echo "Building Rust extension..."
	cd tests/integration/rust_extension && cargo build --release
	@cp tests/integration/rust_extension/target/release/libintegration_ext.so $(BIN_DIR)/libmyext.so
	@echo "Building C leak check program..."
	cd tests/integration/rust_extension && \
		gcc leak_check.c -o $(BIN_DIR)/leak_check -lsqlite3
	@echo "Running Valgrind memory leak check on C program..."
	valgrind --leak-check=full --show-leak-kinds=all --errors-for-leak-kinds=definite $(BIN_DIR)/leak_check

coverage:
	cargo install cargo-tarpaulin || true
	cargo tarpaulin --ignore-tests

leak-check-valgrind:
	cargo install cargo-valgrind || true
	VALGRIND_OPTS="--suppressions=valgrind.supp" cargo valgrind test

clean:
	cargo clean
	rm -rf $(BIN_DIR)
