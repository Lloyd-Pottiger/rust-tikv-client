export RUSTFLAGS=-Dwarnings
export RUSTDOCFLAGS=-Dwarnings

.PHONY: default check unit-test generate integration-test integration-test-txn integration-test-raw integration-test-smoke tiup-integration-test tiup-integration-test-txn tiup-integration-test-raw tiup-integration-test-smoke test doc doc-test coverage coverage-integration tiup-coverage-integration tiup tiup-up tiup-down tiup-clean all clean

export PD_ADDRS     ?= 127.0.0.1:2379
export MULTI_REGION ?= 1
export TIKV_VERSION ?= v8.5.1
export TIUP_KV      ?= 3

ALL_FEATURES := integration-tests

NEXTEST_ARGS := --config-file $(shell pwd)/config/nextest.toml

INTEGRATION_TEST_ARGS := --features "integration-tests" --test-threads 1

RUN_INTEGRATION_TEST := cargo nextest run ${NEXTEST_ARGS} --all ${INTEGRATION_TEST_ARGS}

# Fallback commands when `cargo nextest` isn't installed.
CARGO_TEST_UNIT := cargo test --workspace --no-default-features --lib --tests
CARGO_TEST_INTEGRATION := cargo test --workspace --features "integration-tests" --tests
CARGO_TEST_INTEGRATION_RUNNER_ARGS := -- --test-threads 1

TIUP_PID_FILE := target/tiup-playground.pid
TIUP_NAME_FILE := target/tiup-playground.name
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

tiup-integration-test-smoke:
	@set -e; \
	trap '$(MAKE) tiup-down >/dev/null' EXIT; \
	$(MAKE) tiup-up; \
	$(MAKE) integration-test-smoke

tiup-integration-test:
	@set -e; \
	trap '$(MAKE) tiup-down >/dev/null' EXIT; \
	$(MAKE) tiup-up; \
	$(MAKE) integration-test

tiup-integration-test-txn:
	@set -e; \
	trap '$(MAKE) tiup-down >/dev/null' EXIT; \
	$(MAKE) tiup-up; \
	$(MAKE) integration-test-txn

tiup-integration-test-raw:
	@set -e; \
	trap '$(MAKE) tiup-down >/dev/null' EXIT; \
	$(MAKE) tiup-up; \
	$(MAKE) integration-test-raw

test: unit-test integration-test

doc:
	cargo doc --workspace --exclude tikv-client-proto-build --document-private-items --no-deps

doc-test:
	cargo test --package tikv-client --doc --features "${ALL_FEATURES}"

coverage:
	@if cargo llvm-cov --version >/dev/null 2>&1; then \
		rustup component add llvm-tools-preview >/dev/null 2>&1 || true; \
		cargo llvm-cov --package tikv-client --no-default-features --lib --tests \
			--ignore-filename-regex 'src/generated/' \
			--fail-under-lines 80 \
			--html; \
		echo "Coverage report: target/llvm-cov/html/index.html"; \
	else \
		# Keep the suggested version compatible with rust-toolchain.toml (MSRV). \
		echo "cargo llvm-cov not installed. Install with: cargo install cargo-llvm-cov --version 0.6.21 --locked"; \
		exit 1; \
	fi

coverage-integration:
	@if cargo llvm-cov --version >/dev/null 2>&1; then \
		rustup component add llvm-tools-preview >/dev/null 2>&1 || true; \
		PD_ADDRS="$(PD_ADDRS)" cargo llvm-cov --package tikv-client --no-default-features --features "integration-tests" --lib --tests \
			--ignore-filename-regex 'src/generated/' \
			--fail-under-lines 80 \
			--html -- --test-threads 1; \
		echo "Coverage report: target/llvm-cov/html/index.html"; \
	else \
		# Keep the suggested version compatible with rust-toolchain.toml (MSRV). \
		echo "cargo llvm-cov not installed. Install with: cargo install cargo-llvm-cov --version 0.6.21 --locked"; \
		exit 1; \
	fi

tiup-coverage-integration:
	@set -e; \
	trap '$(MAKE) tiup-down >/dev/null' EXIT; \
	$(MAKE) tiup-up; \
	$(MAKE) coverage-integration

tiup-up:
	@mkdir -p target
	@set -e; \
	kv_cfg="$(shell pwd)/config/tikv.toml"; \
	pd_cfg="$(shell pwd)/config/pd.toml"; \
	name=""; \
	if [ -f "$(TIUP_NAME_FILE)" ]; then name="$$(cat "$(TIUP_NAME_FILE)")"; fi; \
	if [ -n "$$name" ]; then \
		pid=$$(tiup status | awk -v name="$$name" 'NR>2 && $$1==name && $$2=="playground" && $$4=="RUNNING"{print $$3; exit}'); \
		if [ -n "$$pid" ]; then \
			echo "$$pid" > "$(TIUP_PID_FILE)"; \
			echo "tiup playground already running (name=$$name, pid=$$pid)"; \
			echo "Logs: $(TIUP_LOG_FILE)"; \
		else \
			name=""; \
		fi; \
	fi; \
	if [ -z "$$name" ]; then \
		# Recover from a stale/missing name file by matching our config paths in `tiup status`. \
		line=$$(tiup status | awk -v kv="$$kv_cfg" -v pd="$$pd_cfg" 'NR>2 && $$2=="playground" && $$4=="RUNNING" && index($$0, kv) && index($$0, pd) {print $$1" "$$3; exit}'); \
		if [ -n "$$line" ]; then \
			name=$${line%% *}; \
			pid=$${line#* }; \
			echo "$$name" > "$(TIUP_NAME_FILE)"; \
			echo "$$pid" > "$(TIUP_PID_FILE)"; \
			echo "tiup playground already running (name=$$name, pid=$$pid)"; \
			echo "Logs: $(TIUP_LOG_FILE)"; \
		fi; \
	fi; \
	if [ -z "$$name" ]; then \
		rm -f "$(TIUP_NAME_FILE)" "$(TIUP_PID_FILE)"; \
		echo "Starting tiup playground (tikv-slim, $(TIKV_VERSION))..." ; \
		tiup install tikv:$(TIKV_VERSION) pd:$(TIKV_VERSION) ; \
		tiup playground $(TIKV_VERSION) --mode tikv-slim --kv $(TIUP_KV) --without-monitor \
			--kv.config "$$kv_cfg" \
			--pd.config "$$pd_cfg" \
			> "$(TIUP_LOG_FILE)" 2>&1 & \
		launched_pid=$$!; \
		echo "Started tiup (pid=$$launched_pid). Logs: $(TIUP_LOG_FILE)"; \
		echo "Waiting for playground instance to appear in \`tiup status\`..."; \
		tries=0; \
		while [ $$tries -lt 60 ]; do \
			line=$$(tiup status | awk -v kv="$$kv_cfg" -v pd="$$pd_cfg" 'NR>2 && $$2=="playground" && $$4=="RUNNING" && index($$0, kv) && index($$0, pd) {print $$1" "$$3; exit}'); \
			if [ -n "$$line" ]; then \
				name=$${line%% *}; \
				pid=$${line#* }; \
				echo "$$name" > "$(TIUP_NAME_FILE)"; \
				echo "$$pid" > "$(TIUP_PID_FILE)"; \
				echo "Playground started (name=$$name, pid=$$pid)."; \
				break; \
			fi; \
			sleep 1; \
			tries=$$((tries + 1)); \
		done; \
		if [ -z "$$name" ]; then \
			echo "Failed to detect playground instance. See $(TIUP_LOG_FILE)."; \
			exit 1; \
		fi; \
	fi; \
	echo "PD_ADDRS=$(PD_ADDRS)"; \
	pd_addr="$${PD_ADDRS%%,*}"; \
	pd_url="http://$$pd_addr"; \
	case "$$pd_addr" in \
		http://*|https://*) pd_url="$$pd_addr" ;; \
	esac; \
		if ! command -v curl >/dev/null 2>&1; then \
			echo "curl not found; skipping PD readiness check (install curl or wait manually)."; \
			exit 0; \
		fi; \
		echo "Waiting for PD ($$pd_addr) to be ready..."; \
		tries=0; \
		while [ $$tries -lt 60 ]; do \
			if curl -fsS "$$pd_url/pd/api/v1/version" >/dev/null 2>&1; then \
				echo "PD is ready."; \
				break; \
			fi; \
			sleep 1; \
			tries=$$((tries + 1)); \
		done; \
		if [ $$tries -ge 60 ]; then \
			echo "PD did not become ready in time. See $(TIUP_LOG_FILE)."; \
			exit 1; \
		fi; \
		py=""; \
		if command -v python3 >/dev/null 2>&1; then py=python3; elif command -v python >/dev/null 2>&1; then py=python; fi; \
		if [ -z "$$py" ]; then \
			echo "python not found; skipping TiKV stores readiness check."; \
			exit 0; \
		fi; \
		echo "Waiting for TiKV stores to be ready (expected=$(TIUP_KV))..."; \
		expected_kv="$(TIUP_KV)"; \
		tries=0; \
		while [ $$tries -lt 60 ]; do \
			if curl -fsS "$$pd_url/pd/api/v1/stores" 2>/dev/null | $$py -c "import json,sys; expected=int('$$expected_kv'); data=json.load(sys.stdin); count=int(data.get('count',0) or 0); stores=data.get('stores',[]); all_up=all((s.get('store') or {}).get('state_name')=='Up' for s in stores); sys.exit(0 if (count >= expected and all_up) else 1)"; then \
				echo "TiKV stores are ready."; \
				exit 0; \
			fi; \
			sleep 1; \
			tries=$$((tries + 1)); \
		done; \
		echo "TiKV stores did not become ready in time. See $(TIUP_LOG_FILE)."; \
		exit 1

tiup-down:
	@set -e; \
	kv_cfg="$(shell pwd)/config/tikv.toml"; \
	pd_cfg="$(shell pwd)/config/pd.toml"; \
	name=""; \
	if [ -f "$(TIUP_NAME_FILE)" ]; then name="$$(cat "$(TIUP_NAME_FILE)")"; fi; \
	if [ -z "$$name" ]; then \
		# If the name file is missing, try to find the instance that uses our config paths. \
		names=$$(tiup status | awk -v kv="$$kv_cfg" -v pd="$$pd_cfg" 'NR>2 && $$2=="playground" && index($$0, kv) && index($$0, pd) {print $$1}'); \
		count=$$(printf "%s\\n" "$$names" | awk 'NF{c++} END{print c}'); \
		if [ "$$count" -eq 1 ]; then \
			name="$$names"; \
		elif [ "$$count" -gt 1 ]; then \
			echo "Multiple tiup playground instances match our config paths; please stop them manually via \`tiup clean <name>\`:"; \
			printf "%s\\n" "$$names"; \
			exit 1; \
		fi; \
	fi; \
	if [ -n "$$name" ]; then \
		echo "Stopping tiup playground (name=$$name)..." ; \
		tiup clean "$$name" >/dev/null 2>&1 || true; \
		rm -f "$(TIUP_PID_FILE)" "$(TIUP_NAME_FILE)"; \
	else \
		echo "No playground instance found to stop."; \
	fi

tiup-clean: tiup-down
	rm -f "$(TIUP_LOG_FILE)"

tiup: tiup-up

all: generate check doc-test doc test

clean:
	cargo clean
	rm -rf target
