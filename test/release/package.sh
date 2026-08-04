#!/bin/sh
set -eu

version=${1:?usage: package.sh VERSION [OUTPUT_DIRECTORY]}
output=${2:-dist}
numeric_version=${version#v}
commit=${GITHUB_SHA:-unknown}
build_date=${SOURCE_DATE_EPOCH:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)

case "$numeric_version" in
  *[!0-9.]*|'') echo "release version must be numeric or v-prefixed numeric: $version" >&2; exit 2 ;;
esac

cd "$root"
chart_version=$(awk '$1 == "version:" { print $2; exit }' charts/riquet/Chart.yaml)
if [ "$chart_version" != "$numeric_version" ]; then
  echo "chart version $chart_version does not match release $numeric_version" >&2
  exit 1
fi

mkdir -p "$output"
for arch in amd64 arm64; do
  stage=$(mktemp -d)
  for command in riquet riquet-backup riquet-export riquet-restore; do
    package="./cmd/$command"
    ldflags="-s -w"
    if [ "$command" = riquet ]; then
      ldflags="$ldflags -X github.com/k3rnL/riquet/internal/buildinfo.version=$numeric_version -X github.com/k3rnL/riquet/internal/buildinfo.commit=$commit -X github.com/k3rnL/riquet/internal/buildinfo.date=$build_date"
    fi
    CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -trimpath -ldflags "$ldflags" -o "$stage/$command" "$package"
  done
  cp LICENSE THIRD_PARTY_NOTICES.md "$stage/"
  tar -C "$stage" -czf "$output/riquet_${numeric_version}_linux_${arch}.tar.gz" .
  rm -rf "$stage"
done

helm package charts/riquet --destination "$output" >/dev/null
(
  cd "$output"
  sha256sum riquet_*.tar.gz riquet-*.tgz > checksums.txt
)
