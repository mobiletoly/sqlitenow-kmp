#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [[ -n "${SQLITENOW_DOCS_RUBY_HOME:-}" ]]; then
  export PATH="$SQLITENOW_DOCS_RUBY_HOME/bin:$PATH"
elif [[ -x /opt/homebrew/opt/ruby@3.2/bin/ruby ]]; then
  export PATH="/opt/homebrew/opt/ruby@3.2/bin:$PATH"
elif [[ -x /usr/local/opt/ruby@3.2/bin/ruby ]]; then
  export PATH="/usr/local/opt/ruby@3.2/bin:$PATH"
fi

export BUNDLE_PATH="${BUNDLE_PATH:-vendor/bundle}"

bundle check || bundle install

exec bundle exec jekyll serve \
  --host "${JEKYLL_HOST:-127.0.0.1}" \
  --port "${JEKYLL_PORT:-4000}"
