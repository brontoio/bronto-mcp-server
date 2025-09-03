#!/usr/bin/env bash
set -e

PROJECT_ROOT_DIR="${PWD}"
REQUIREMENT_FILE="${PROJECT_ROOT_DIR}/requirements.txt"
TEST_REQUIREMENT_FILE="${PROJECT_ROOT_DIR}/test_requirements.txt"
if [ ! -f "${TEST_REQUIREMENT_FILE}" ]
then
  echo "${TEST_REQUIREMENT_FILE} is missing. Cannot run Ruff, Pytest, etc. Skipping."
  exit 0
fi
python3 -m venv test_env
PYTHONPATH="${PROJECT_ROOT_DIR}/src/main/brmcpserver" "${PROJECT_ROOT_DIR}/test_env/bin/python" -m pip install --upgrade pip
test_env/bin/pip install -r test_requirements.txt
if [ -f "${REQUIREMENT_FILE}" ]
then
  test_env/bin/pip install -r requirements.txt
fi

cd "${PROJECT_ROOT_DIR}/src/test" || exit 1

echo "Running Ruff"
PYTHONPATH="${PROJECT_ROOT_DIR}/src/main/brmcpserver" "${PROJECT_ROOT_DIR}/test_env/bin/ruff" format || true
echo "Running Tests"
PYTHONPATH="${PROJECT_ROOT_DIR}/src/main/brmcpserver" "${PROJECT_ROOT_DIR}/test_env/bin/coverage" run -m pytest -v -s
echo "Reporting Test Coverage"
PYTHONPATH="${PROJECT_ROOT_DIR}/src/main/brmcpserver" "${PROJECT_ROOT_DIR}/test_env/bin/coverage" report -m
