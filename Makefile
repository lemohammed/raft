PREFIX ?= $(HOME)/.local
BINDIR ?= $(PREFIX)/bin

.PHONY: build release install uninstall test lint fmt check clean

build:
	cargo build

release:
	cargo build --release

install: release
	./scripts/install.sh "$(BINDIR)"

uninstall:
	rm -f "$(BINDIR)/raft"

test:
	cargo test

lint:
	cargo clippy -- -D warnings

fmt:
	cargo fmt

check: fmt lint test

clean:
	cargo clean
