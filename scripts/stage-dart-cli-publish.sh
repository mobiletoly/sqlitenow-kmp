#!/usr/bin/env bash
set -euo pipefail

mode="${1:---dry-run}"
if [[ "${mode}" != "--dry-run" && "${mode}" != "--publish" ]]; then
  echo "Usage: scripts/stage-dart-cli-publish.sh [--dry-run|--publish]" >&2
  exit 64
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
package_dir="${repo_root}/dart/packages/sqlitenow_cli"
jar_path="${package_dir}/lib/src/compiler/sqlitenow-compiler.jar"

(cd "${repo_root}/sqlitenow-compiler" && ../gradlew syncDartCliCompilerJar)

if [[ ! -f "${jar_path}" ]]; then
  echo "Compiler jar was not created: ${jar_path}" >&2
  exit 1
fi

stage_dir="$(mktemp -d "${TMPDIR:-/tmp}/sqlitenow-dart-cli-publish.XXXXXX")"
trap 'rm -rf "${stage_dir}"' EXIT

staged_package="${stage_dir}/sqlitenow_cli"
mkdir -p "${staged_package}"
rsync -a \
  --exclude='.dart_tool/' \
  --exclude='build/' \
  "${package_dir}/" \
  "${staged_package}/"

mkdir -p "${staged_package}/lib/src/compiler"
cp "${jar_path}" "${staged_package}/lib/src/compiler/sqlitenow-compiler.jar"
perl -0pi -e 's/\nresolution: workspace\n/\n/' "${staged_package}/pubspec.yaml"

if [[ "${mode}" == "--dry-run" ]]; then
  (cd "${staged_package}" && dart pub publish --dry-run --ignore-warnings)
else
  (cd "${staged_package}" && dart pub publish --ignore-warnings)
fi
