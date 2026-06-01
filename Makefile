PREFIX ?= $(HOME)/.local
BINDIR ?= $(PREFIX)/bin
TOOLCHAIN ?= 1.88.0
CARGO ?= rustup run $(TOOLCHAIN) cargo

.PHONY: build release install uninstall test lint fmt fmt-check temp-policy check clean toolchain

toolchain:
	rustup toolchain install $(TOOLCHAIN) --profile minimal --component clippy,rustfmt

build:
	$(CARGO) build --locked

release:
	$(CARGO) build --release --locked

install: release
	./scripts/install.sh "$(BINDIR)"

uninstall:
	rm -f "$(BINDIR)/raft"

test:
	$(CARGO) test --locked

lint:
	$(CARGO) clippy --locked --all-targets --all-features -- -D warnings

fmt:
	$(CARGO) fmt

fmt-check:
	$(CARGO) fmt --check

temp-policy:
	! rg -n '(/tmp|/private/tmp|/var/tmp|std::env::temp_dir|TMPDIR|\.join\("tmp"\))' README.md docs src tests scripts

check: fmt-check lint test temp-policy

clean:
	$(CARGO) clean
