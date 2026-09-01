lint:
	cargo fmt && cargo clippy --all-targets -- -D warnings

test:
	cargo test

build:
	cargo build
