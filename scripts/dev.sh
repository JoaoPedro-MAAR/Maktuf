#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

GOLANGCI_LINT_VERSION="${GOLANGCI_LINT_VERSION:-v1.61.0}"
GOLANGCI_LINT="$ROOT/bin/golangci-lint"

go_files() {
  git ls-files '*.go'
}

case "${1:-}" in
  setup)
    git config core.hooksPath .githooks
    chmod +x .githooks/pre-commit
    echo "Hook de pre-commit ativado."
    ;;
  fmt)
    mapfile -t files < <(go_files)
    if [ "${#files[@]}" -eq 0 ]; then
      echo "Nenhum arquivo Go encontrado."
    else
      gofmt -w "${files[@]}"
    fi
    ;;
  vet)
    if [ -z "$(go_files)" ]; then
      echo "Nenhum arquivo Go encontrado."
    else
      go vet ./...
    fi
    ;;
  lint)
    if [ -z "$(go_files)" ]; then
      echo "Nenhum arquivo Go encontrado."
      exit 0
    fi
    if [ -x "$GOLANGCI_LINT" ]; then
      "$GOLANGCI_LINT" run ./...
    elif command -v golangci-lint >/dev/null; then
      golangci-lint run ./...
    else
      echo "golangci-lint nao encontrado. Rode: ./scripts/dev.sh tools"
      exit 1
    fi
    ;;
  test)
    if [ -z "$(go_files)" ]; then
      echo "Nenhum arquivo Go encontrado."
    else
      go test ./...
    fi
    ;;
  check)
    "$0" fmt
    "$0" vet
    "$0" lint
    ;;
  tools)
    GOBIN="$ROOT/bin" go install \
      github.com/golangci/golangci-lint/cmd/golangci-lint@"$GOLANGCI_LINT_VERSION"
    ;;
  *)
    echo "Uso: $0 {setup|fmt|vet|lint|test|check|tools}"
    exit 1
    ;;
esac
