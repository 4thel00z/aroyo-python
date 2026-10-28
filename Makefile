.DEFAULT_GOAL := help

##@ Rust

.PHONY: build
build: ## Build the workspace
	cargo build --workspace

.PHONY: fmt
fmt: ## Format every crate
	cargo fmt --all

.PHONY: fmt-check
fmt-check: ## Fail on unformatted code
	cargo fmt --all -- --check

.PHONY: lint
lint: ## Clippy with warnings denied
	cargo clippy --workspace --all-targets -- -D warnings

.PHONY: test
test: ## Run the Rust tests (in-memory object store, no network)
	cargo test --workspace

.PHONY: ci
ci: fmt-check lint test ## Everything the Rust CI runs

##@ Python

.PHONY: develop
develop: ## Build the extension into the active venv
	maturin develop

.PHONY: test-py
test-py: ## Run pytest against an in-process moto S3 (or AROYO_TEST_ENDPOINT)
	pytest -q

.PHONY: minio
minio: ## Run a local MinIO for integration tests and benchmarks
	docker run --rm -p 9000:9000 -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data

.PHONY: bench
bench: ## Throughput benchmark; needs AROYO_BENCH_ENDPOINT and AROYO_BENCH_BUCKET
	python benchmarks/bench_throughput.py

.PHONY: help
help:
	@awk 'BEGIN {FS = ":.*##"} /^##@/ {printf "\n%s\n", substr($$0, 5)} /^[a-zA-Z_-]+:.*?##/ {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
