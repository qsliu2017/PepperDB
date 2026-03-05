#!/bin/bash
set -e

# Verify Rust toolchain
rustc --version
cargo --version

# Verify PostgreSQL source is available
echo "PostgreSQL source available at /opt/postgres-src"
head -1 /opt/postgres-src/COPYRIGHT

echo ""
echo "PepperDB dev container ready!"
echo "  - Rust toolchain: $(rustc --version)"
echo "  - PostgreSQL 18 source: /opt/postgres-src"
echo "  - Claude Code: $(claude --version 2>/dev/null || echo 'run claude to authenticate')"
