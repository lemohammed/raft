#!/usr/bin/env sh
set -eu

BINDIR="${1:-"$HOME/.local/bin"}"
ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
TARGET="$BINDIR/raft"
STABLE_BIN="$ROOT_DIR/bin/raft-release"
RELEASE_BIN="$ROOT_DIR/target/release/raft"

mkdir -p "$BINDIR"
mkdir -p "$ROOT_DIR/bin"

if [ ! -x "$RELEASE_BIN" ]; then
  printf 'missing release binary: %s\n' "$RELEASE_BIN" >&2
  printf 'run: cargo build --release\n' >&2
  exit 1
fi

tmp_bin="$(mktemp "$ROOT_DIR/bin/.raft-release.XXXXXX")"
cp "$RELEASE_BIN" "$tmp_bin"
chmod 755 "$tmp_bin"
mv "$tmp_bin" "$STABLE_BIN"

tmp_shim="$(mktemp "$BINDIR/.raft.XXXXXX")"
cat > "$tmp_shim" <<EOF
#!/usr/bin/env sh
set -eu

: "\${RAFT_ROOT:=/Users/mohamad.hassan/workspace/raft/run/bus}"
export RAFT_ROOT

exec /Users/mohamad.hassan/workspace/raft/bin/raft "\$@"
EOF

chmod 755 "$tmp_shim"
mv "$tmp_shim" "$TARGET"

printf 'installed raft shim at %s\n' "$TARGET"
printf 'installed stable raft binary at %s\n' "$STABLE_BIN"
printf 'default RAFT_ROOT=%s\n' "/Users/mohamad.hassan/workspace/raft/run/bus"
