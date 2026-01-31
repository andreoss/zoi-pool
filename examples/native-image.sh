#!/bin/sh
set -eu

out="${1:-target/native/basic-usage}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

mkdir -p "$(dirname "$out")"
sbt -batch -error "export examples/Runtime/fullClasspath" > "$work/exported"
awk '/examples\/target/ { line = $0 } END { print line }' "$work/exported" > "$work/classpath"

classpath="$(cat "$work/classpath")"
case "$classpath" in
  *zio*:*izumi*|*izumi*:*zio*)
    ;;
  *)
    echo "the exported classpath does not carry the example's dependencies:" >&2
    cat "$work/exported" >&2
    exit 1
    ;;
esac

native-image --no-fallback -cp "$classpath" -o "$out" zoi.pool.examples.BasicUsage
"$out"
