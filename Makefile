# Makefile — common tasks for ironcondor

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the project (all features)
	cargo build --all-features

.PHONY: release
release: ## Build the release profile
	cargo build --release

.PHONY: test
test: ## Run the test suite (all features)
	cargo test --all-features

.PHONY: fmt
fmt: ## Format the code
	cargo fmt --all

.PHONY: fmt-check
fmt-check: ## Check formatting without modifying
	cargo fmt --all --check

.PHONY: lint
lint: ## Run clippy with warnings denied
	cargo clippy --all-targets --all-features --workspace -- -D warnings

.PHONY: lint-fix
lint-fix: ## Run clippy and apply fixes
	cargo clippy --fix --all-targets --all-features --allow-staged --allow-dirty --workspace -- -D warnings

.PHONY: fix
fix: ## Apply rustc suggestions
	cargo fix --allow-staged --allow-dirty

.PHONY: doc
doc: ## Build the documentation
	cargo doc --no-deps --document-private-items

.PHONY: check-cargo-readme
check-cargo-readme:
	@command -v cargo-readme > /dev/null || (echo "Installing cargo-readme..."; cargo install cargo-readme)

.PHONY: readme
readme: check-cargo-readme ## Regenerate README.md from src/lib.rs docs (never hand-edit)
	cargo readme > README.md

.PHONY: check-spanish
check-spanish: ## Fail if Spanish text appears in code comments
	@if rg -q --pcre2 -e '^\s*(//|///|//!|#|/\*|\*).*?[áéíóúÁÉÍÓÚñÑ¿¡]' --glob '!target/*' . 2>/dev/null; then \
		echo "❌  Spanish comments found:"; \
		rg -n --pcre2 -e '^\s*(//|///|//!|#|/\*|\*).*?[áéíóúÁÉÍÓÚñÑ¿¡]' --glob '!target/*' .; \
		exit 1; \
	else \
		echo "✅  No Spanish comments"; \
	fi

.PHONY: pre-push
pre-push: fix fmt lint-fix test release readme doc check-spanish ## The pre-push gate: fix + fmt + lint-fix + test + release + readme + doc + check-spanish

.PHONY: clean
clean: ## Remove build artifacts
	cargo clean
