#!/bin/sh
set -eu

out="${1:-target/native/basic-usage}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

mkdir -p "$(dirname "$out")"
sbt -batch -error "export examples/Runtime/fullClasspath" > "$work/classpath"
native-image --no-fallback -cp "$(cat "$work/classpath")" -o "$out" \
  zoi.pool.examples.BasicUsage
"$out"
