export RUSTFLAGS=-Dwarnings
export RUSTDOCFLAGS=-Dwarnings

.PHONY: default check unit-test generate integration-test integration-test-txn integration-test-raw integration-test-smoke test doc tiup tiup-up tiup-down tiup-clean all clean

export PD_ADDRS     ?= 127.0.0.1:2379
export MULTI_REGION ?= 1

ALL_FEATURES := integration-tests

NEXTEST_ARGS := --config-file $(shell pwd)/config/nextest.toml

INTEGRATION_TEST_ARGS := --features "integration-tests" --test-threads 1

RUN_INTEGRATION_TEST := cargo nextest run ${NEXTEST_ARGS} --all ${INTEGRATION_TEST_ARGS}

# Fallback commands when `cargo nextest` isn't installed.
CARGO_TEST_UNIT := cargo test --workspace --no-default-features --lib --tests
CARGO_TEST_INTEGRATION := cargo test --workspace --features "integration-tests" --tests
CARGO_TEST_INTEGRATION_RUNNER_ARGS := -- --test-threads 1

TIUP_PID_FILE := target/tiup-playground.pid
TIUP_LOG_FILE := target/tiup-playground.log

default: check

generate:
	cargo run -p tikv-client-proto-build

check: generate
	cargo check --all --all-targets --features "${ALL_FEATURES}"
	cargo fmt -- --check
	cargo clippy --all-targets --features "${ALL_FEATURES}" -- -D clippy::all

unit-test: generate
	@if cargo nextest --version >/dev/null 2>&1; then \
		cargo nextest run ${NEXTEST_ARGS} --all --no-default-features; \
	else \
		$(CARGO_TEST_UNIT); \
	fi

integration-test: integration-test-txn integration-test-raw

integration-test-txn: generate
	@if cargo nextest --version >/dev/null 2>&1; then \
		$(RUN_INTEGRATION_TEST) txn_; \
	else \
		$(CARGO_TEST_INTEGRATION) txn_ $(CARGO_TEST_INTEGRATION_RUNNER_ARGS); \
	fi

integration-test-raw: generate
	@if cargo nextest --version >/dev/null 2>&1; then \
		$(RUN_INTEGRATION_TEST) raw_; \
	else \
		$(CARGO_TEST_INTEGRATION) raw_ $(CARGO_TEST_INTEGRATION_RUNNER_ARGS); \
	fi

integration-test-smoke: generate
	PD_ADDRS="$(PD_ADDRS)" cargo test --package tikv-client --test integration_tests --features integration-tests txn_crud -- --test-threads 1
	PD_ADDRS="$(PD_ADDRS)" cargo test --package tikv-client --test integration_tests --features integration-tests txn_try_one_pc -- --test-threads 1
	PD_ADDRS="$(PD_ADDRS)" cargo test --package tikv-client --test integration_tests --features integration-tests txn_pipelined_flush -- --test-threads 1
	PD_ADDRS="$(PD_ADDRS)" cargo test --package tikv-client --test integration_tests --features integration-tests txn_replica_read_snapshot_get -- --test-threads 1
	PD_ADDRS="$(PD_ADDRS)" cargo test --package tikv-client --test integration_tests --features integration-tests raw_req -- --test-threads 1
	PD_ADDRS="$(PD_ADDRS)" cargo test --package tikv-client --test integration_tests --features integration-tests raw_checksum -- --test-threads 1

test: unit-test integration-test

doc:
	cargo doc --workspace --exclude tikv-client-proto-build --document-private-items --no-deps

tiup-up:
	@mkdir -p target
	@if [ -f "$(TIUP_PID_FILE)" ] && kill -0 "$$(cat "$(TIUP_PID_FILE)")" 2>/dev/null; then \
		echo "tiup playground already running (pid=$$(cat "$(TIUP_PID_FILE)"))"; \
		exit 0; \
	fi
	@echo "Starting tiup playground (tikv-slim)..." ; \
		tiup playground nightly --mode tikv-slim --kv 3 --without-monitor \
			--kv.config "$(shell pwd)/config/tikv.toml" \
			--pd.config "$(shell pwd)/config/pd.toml" \
			> "$(TIUP_LOG_FILE)" 2>&1 & \
		echo $$! > "$(TIUP_PID_FILE)"
	@echo "Started (pid=$$(cat "$(TIUP_PID_FILE)")). Logs: $(TIUP_LOG_FILE)"
	@echo "PD_ADDRS=$(PD_ADDRS)"

tiup-down:
	@if [ -f "$(TIUP_PID_FILE)" ]; then \
		pid=$$(cat "$(TIUP_PID_FILE)"); \
		echo "Stopping tiup playground (pid=$$pid)..." ; \
		kill $$pid 2>/dev/null || true; \
		rm -f "$(TIUP_PID_FILE)"; \
	else \
		echo "No pid file: $(TIUP_PID_FILE)"; \
	fi

tiup-clean: tiup-down
	rm -f "$(TIUP_LOG_FILE)"

tiup: tiup-up

all: generate check doc test

clean:
	cargo clean
	rm -rf target
