#!/bin/sh

source venv/bin/activate

echo "Running mypy type checks..."
python3 -m mypy --explicit-package-bases --check-untyped-defs application common core generated

exit_code=$?
if [ $exit_code -ne 0 ]; then
  echo "❌ Mypy validation failed. Please fix the type errors."
  exit 1
fi

echo "✅ Mypy validation passed."
exit 0
