#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
expected="$root/test/release/dependency-licenses.tsv"
actual=$(mktemp)
inventory=$(mktemp)
trap 'rm -f "$actual" "$inventory"' EXIT HUP INT TERM

cd "$root"
go list -deps -f '{{with .Module}}{{if not .Main}}{{.Path}} {{.Version}}{{end}}{{end}}' ./cmd/... \
  | awk 'NF == 2 { print $1 "\t" $2 }' \
  | sort -u > "$actual"
cut -f1,2 "$expected" > "$inventory"

if ! diff -u "$inventory" "$actual"; then
  echo "release dependency inventory changed; review its license and update dependency-licenses.tsv" >&2
  exit 1
fi

while IFS="$(printf '\t')" read -r module version license; do
  directory=$(go list -m -f '{{.Dir}}' "$module@$version")
  license_file=$(find "$directory" -maxdepth 1 -type f \
    \( -iname 'license*' -o -iname 'licence*' -o -iname 'copying*' \) \
    | sort | head -1)
  if [ -z "$license_file" ]; then
    echo "$module $version has no distributable license file" >&2
    exit 1
  fi
  case "$license" in
    Apache-2.0|BSD-2-Clause|BSD-3-Clause|ISC|MIT|MPL-2.0|'MIT AND Apache-2.0') ;;
    *) echo "$module $version uses unapproved license expression: $license" >&2; exit 1 ;;
  esac
  printf '%s\t%s\t%s\t%s\n' "$module" "$version" "$license" "$(basename "$license_file")"
done < "$expected"
