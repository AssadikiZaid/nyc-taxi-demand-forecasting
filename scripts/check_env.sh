#!/usr/bin/env bash
# Check that the tools needed by the project are installed. Prints one line per tool.
set -u

status=0

ok() { printf '  \033[32mOK\033[0m       %s\n' "$1"; }
missing() { printf '  \033[31mMISSING\033[0m  %s\n' "$1"; status=1; }

echo "Checking the development environment:"

for tool in git make curl; do
    if command -v "$tool" >/dev/null 2>&1; then ok "$tool"; else missing "$tool"; fi
done

if command -v uv >/dev/null 2>&1; then
    ok "uv $(uv --version | awk '{print $2}')"
    if uv python find 3.12 >/dev/null 2>&1; then
        ok "Python 3.12 ($(uv python find 3.12))"
    else
        missing "Python 3.12 (install it with: uv python install 3.12)"
    fi
else
    missing "uv (see docs/development_setup.md)"
fi

if command -v java >/dev/null 2>&1; then
    java_major=$(java -version 2>&1 | awk -F '"' '/version/ {split($2, v, "."); print v[1]; exit}')
    if [ -n "$java_major" ] && [ "$java_major" -ge 17 ]; then
        ok "Java $java_major (Spark 4 needs 17 or later)"
    else
        missing "Java 17 or later, found '${java_major:-unknown}' (needed from phase 1 for PySpark)"
    fi
else
    missing "Java 17 or later (needed from phase 1 for PySpark)"
fi

if [ "$status" -eq 0 ]; then
    echo "All good."
else
    echo "Some tools are missing: see docs/development_setup.md."
fi
exit "$status"
