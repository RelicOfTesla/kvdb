#!/usr/bin/env bash
# 本仓库的 Go 命令包装：沙箱/开发机默认 GOPATH 模块缓存与 sumdb 目录可能只读，
# 统一在工作区内使用独立的模块/构建缓存与临时目录，并关闭在线 sumdb 校验
# （模块哈希仍由镜像代理提供并写入 go.sum，见 README"构建环境"）。
# 用法：scripts/go.sh <go 子命令...>
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export GOMODCACHE="${GOMODCACHE:-$ROOT/.gomodcache}"
export GOCACHE="${GOCACHE:-$ROOT/.gocache}"
export GOTMPDIR="${GOTMPDIR:-$ROOT/.gotmp}"
export GOSUMDB="${GOSUMDB:-off}"
mkdir -p "$GOMODCACHE" "$GOCACHE" "$GOTMPDIR"
exec go "$@"