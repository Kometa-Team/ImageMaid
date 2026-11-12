#!/usr/bin/env bash
# Reads changed file paths (one per line) on stdin and prints build=true|false for $GITHUB_OUTPUT; shared by ci.yml and increment-build.yml.
set -euo pipefail

build=false

while IFS= read -r file; do
  [[ -z "$file" ]] && continue
  case "$file" in
    imagemaid.py* | requirements.txt* | .dockerignore* | Dockerfile*)
      echo "$file will trigger docker build" >&2
      build=true
      ;;
    *)
      echo "$file will not trigger docker build" >&2
      ;;
  esac
done

echo "build=$build"
