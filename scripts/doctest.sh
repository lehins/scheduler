#!/usr/bin/env bash

set -euo pipefail

cabal repl scheduler --build-depends=QuickCheck --build-depends=mwc-random --with-compiler=doctest --repl-options='-w -Wdefault'
