#!/usr/bin/env bash
# Checks the exported API against the newest release tag with gorelease (ADR-0020 rows 1 and 3).
#
# Fails on any incompatible change except a method added to one of the interfaces that ADR-0020
# row 3 lets grow in a minor release: Lease, LeaseObserver and VacuumObserver in the root package,
# and Observer in pool. When HEAD_REF is release/vX.Y.Z, also checks that vX.Y.Z is a valid version
# for the changes. Run from the repository root on a clean, committed tree with full tag history.
set -euo pipefail

module=github.com/aetomala/worklease

# ===== STEP 1: Choose the base and the release version =====
base=$(git tag --list 'v[0-9]*' --sort=-v:refname | head -n 1)
if [[ -z "$base" ]]; then
  echo "no release tag found" >&2
  exit 1
fi
version=""
if [[ "${HEAD_REF:-}" =~ ^release/(v[0-9]+\.[0-9]+\.[0-9]+)$ ]]; then
  version="${BASH_REMATCH[1]}"
fi
echo "base: $base${version:+, release version: $version}"

# ===== STEP 2: Run gorelease against the base =====
set +e
out=$(gorelease -base="$base" 2>&1)
rc=$?
set -e
echo "$out"

# ===== STEP 3: No incompatible change — let gorelease judge the release version =====
if [[ $rc -eq 0 ]]; then
  if [[ -n "$version" ]]; then
    echo "gorelease -base=$base -version=$version"
    gorelease -base="$base" -version="$version"
  fi
  exit 0
fi

# ===== STEP 4: Incompatible changes — accept only row-3 interface method additions =====
summary=$(awk '/^# summary$/{s=1; next} s && NF' <<<"$out")
if [[ "$summary" != $'Cannot suggest a release version.\nIncompatible changes were detected.' ]]; then
  echo "FAIL: gorelease reported more than incompatible changes" >&2
  exit 1
fi
rejected=$(awk -v m="$module" '
  /^# / { pkg = substr($0, 3); sec = ""; next }
  /^## / { sec = substr($0, 4); next }
  sec == "incompatible changes" && NF {
    if (pkg == m && $0 ~ /^(Lease|LeaseObserver|VacuumObserver)\.[A-Z][A-Za-z0-9_]*: added$/) next
    if (pkg == m "/pool" && $0 ~ /^Observer\.[A-Z][A-Za-z0-9_]*: added$/) next
    print pkg ": " $0
  }' <<<"$out")
if [[ -n "$rejected" ]]; then
  echo "FAIL: incompatible changes outside ADR-0020 row 3:" >&2
  echo "$rejected" >&2
  exit 1
fi
echo "Only methods added to interfaces that ADR-0020 row 3 lets grow in a minor release."

# ===== STEP 5: Row-3 additions need a minor release =====
if [[ -n "$version" ]]; then
  IFS=. read -r bmaj bmin _ <<<"${base#v}"
  IFS=. read -r vmaj vmin _ <<<"${version#v}"
  if [[ "$vmaj" != "$bmaj" || "$vmin" -le "$bmin" ]]; then
    echo "FAIL: $version must raise the minor version of $base: interface methods were added" >&2
    exit 1
  fi
  echo "$version is a valid minor release over $base."
fi
