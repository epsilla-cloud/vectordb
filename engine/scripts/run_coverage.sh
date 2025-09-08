#!/bin/bash

# Coverage Analysis Script for Epsilla VectorDB
# Generates comprehensive code coverage reports for C++ and Python code

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
BUILD_DIR="build"
COVERAGE_DIR="coverage_report"
TARGET_COVERAGE=80

echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}Epsilla VectorDB Coverage Analysis${NC}"
echo -e "${BLUE}======================================${NC}"

# Clean previous coverage data
echo -e "${YELLOW}Cleaning previous coverage data...${NC}"
rm -rf $COVERAGE_DIR
mkdir -p $COVERAGE_DIR
find $BUILD_DIR -name "*.gcda" -delete 2>/dev/null || true

# Build with coverage flags
echo -e "${YELLOW}Building with coverage instrumentation...${NC}"
cmake -B $BUILD_DIR \
  -DCMAKE_BUILD_TYPE=Debug \
  -DENABLE_COVERAGE=ON \
  -DCMAKE_CXX_FLAGS="--coverage -g -O0" \
  -DCMAKE_C_FLAGS="--coverage -g -O0" \
  -DCMAKE_EXE_LINKER_FLAGS="--coverage" \
  -DCMAKE_SHARED_LINKER_FLAGS="--coverage"

cmake --build $BUILD_DIR --parallel $(nproc)

# Run C++ unit tests
echo -e "${YELLOW}Running C++ unit tests...${NC}"
cd $BUILD_DIR
ctest --output-on-failure --parallel $(nproc) || {
  echo -e "${RED}Some C++ tests failed, but continuing with coverage analysis${NC}"
}
cd ..

# Run Python integration tests
echo -e "${YELLOW}Running Python integration tests...${NC}"
export PYTHONPATH=./$BUILD_DIR/
export DB_PATH=/tmp/coverage_testdb
rm -rf $DB_PATH
bash test.sh || {
  echo -e "${RED}Some Python tests failed, but continuing with coverage analysis${NC}"
}

# Generate coverage report with lcov
echo -e "${YELLOW}Generating coverage report...${NC}"

# Capture coverage data
lcov --directory $BUILD_DIR \
     --capture \
     --output-file $COVERAGE_DIR/coverage.info \
     --rc lcov_branch_coverage=1 || {
  echo -e "${RED}Failed to capture coverage data${NC}"
  exit 1
}

# Filter out system headers and test files
lcov --remove $COVERAGE_DIR/coverage.info \
     '/usr/*' \
     '*/build/_deps/*' \
     '*/test/*' \
     '*/thirdparty/*' \
     '*/googletest/*' \
     --output-file $COVERAGE_DIR/coverage_filtered.info \
     --rc lcov_branch_coverage=1

# Generate HTML report
genhtml $COVERAGE_DIR/coverage_filtered.info \
        --output-directory $COVERAGE_DIR/html \
        --title "Epsilla VectorDB Coverage Report" \
        --show-details \
        --highlight \
        --legend \
        --branch-coverage \
        --function-coverage || {
  echo -e "${RED}Failed to generate HTML report${NC}"
  exit 1
}

# Generate detailed summary
echo -e "${BLUE}Coverage Summary:${NC}"
lcov --summary $COVERAGE_DIR/coverage_filtered.info --rc lcov_branch_coverage=1

# Extract coverage percentages
line_coverage=$(lcov --summary $COVERAGE_DIR/coverage_filtered.info 2>/dev/null | grep "lines" | awk '{print $2}' | sed 's/%//')
function_coverage=$(lcov --summary $COVERAGE_DIR/coverage_filtered.info 2>/dev/null | grep "functions" | awk '{print $2}' | sed 's/%//')
branch_coverage=$(lcov --summary $COVERAGE_DIR/coverage_filtered.info 2>/dev/null | grep "branches" | awk '{print $2}' | sed 's/%//' || echo "N/A")

# Convert to integer for comparison (handle decimal values)
line_coverage_int=$(echo $line_coverage | cut -d'.' -f1)
function_coverage_int=$(echo $function_coverage | cut -d'.' -f1)

echo -e "\n${BLUE}Detailed Coverage Analysis:${NC}"
echo -e "Line Coverage:     ${line_coverage}%"
echo -e "Function Coverage: ${function_coverage}%"
echo -e "Branch Coverage:   ${branch_coverage}%"

# Per-directory coverage analysis
echo -e "\n${BLUE}Per-Directory Coverage:${NC}"
lcov --list $COVERAGE_DIR/coverage_filtered.info | grep -E "\.cpp|\.hpp" | while read line; do
  if [[ $line == *"/"* ]]; then
    echo "$line"
  fi
done | head -20

# Generate module-specific reports
echo -e "\n${BLUE}Generating module-specific reports...${NC}"

modules=("db" "utils" "config" "services" "server" "logger" "query")

for module in "${modules[@]}"; do
  if [ -d "$module" ]; then
    echo -e "Analyzing module: $module"
    
    # Extract coverage for this module
    lcov --extract $COVERAGE_DIR/coverage_filtered.info "*/$module/*" \
         --output-file $COVERAGE_DIR/${module}_coverage.info \
         --rc lcov_branch_coverage=1 2>/dev/null || continue
    
    # Generate module-specific HTML report
    genhtml $COVERAGE_DIR/${module}_coverage.info \
            --output-directory $COVERAGE_DIR/html_$module \
            --title "Epsilla VectorDB - $module Module Coverage" \
            --show-details \
            --highlight \
            --legend \
            --branch-coverage \
            --function-coverage 2>/dev/null || continue
    
    # Get module summary
    module_summary=$(lcov --summary $COVERAGE_DIR/${module}_coverage.info 2>/dev/null | grep "lines" | awk '{print $2}' || echo "N/A")
    echo -e "  $module: $module_summary"
  fi
done

# Generate uncovered lines report
echo -e "\n${BLUE}Generating uncovered lines report...${NC}"
lcov --list $COVERAGE_DIR/coverage_filtered.info | \
  awk '/^SF:/ {file=$0; gsub("SF:", "", file)} 
       /^DA:[0-9]+,0$/ {gsub("DA:", "", $0); gsub(",0", "", $0); print file ":" $0}' | \
  head -50 > $COVERAGE_DIR/uncovered_lines.txt

echo -e "Top uncovered lines saved to: $COVERAGE_DIR/uncovered_lines.txt"

# Generate coverage badge data
echo -e "\n${BLUE}Generating coverage badge data...${NC}"
cat > $COVERAGE_DIR/coverage_badge.json << EOF
{
  "schemaVersion": 1,
  "label": "coverage",
  "message": "${line_coverage}%",
  "color": "$(if (( ${line_coverage_int:-0} >= 80 )); then echo "brightgreen"; elif (( ${line_coverage_int:-0} >= 70 )); then echo "yellow"; else echo "red"; fi)"
}
EOF

# Check coverage thresholds
echo -e "\n${BLUE}Coverage Threshold Analysis:${NC}"

if (( ${line_coverage_int:-0} >= $TARGET_COVERAGE )); then
  echo -e "${GREEN}✅ Line coverage (${line_coverage}%) meets target ($TARGET_COVERAGE%)${NC}"
  coverage_status=0
else
  echo -e "${RED}❌ Line coverage (${line_coverage}%) below target ($TARGET_COVERAGE%)${NC}"
  coverage_status=1
fi

if (( ${function_coverage_int:-0} >= $TARGET_COVERAGE )); then
  echo -e "${GREEN}✅ Function coverage (${function_coverage}%) meets target ($TARGET_COVERAGE%)${NC}"
else
  echo -e "${RED}❌ Function coverage (${function_coverage}%) below target ($TARGET_COVERAGE%)${NC}"
  coverage_status=1
fi

# Generate final report
echo -e "\n${BLUE}Coverage Report Generated:${NC}"
echo -e "HTML Report: file://$(pwd)/$COVERAGE_DIR/html/index.html"
echo -e "Coverage Data: $(pwd)/$COVERAGE_DIR/coverage_filtered.info"
echo -e "Uncovered Lines: $(pwd)/$COVERAGE_DIR/uncovered_lines.txt"

# Suggest improvements
if (( coverage_status == 1 )); then
  echo -e "\n${YELLOW}Suggestions to improve coverage:${NC}"
  echo -e "1. Add unit tests for uncovered functions in $COVERAGE_DIR/uncovered_lines.txt"
  echo -e "2. Focus on modules with lowest coverage percentages"
  echo -e "3. Add integration tests for complex workflows"
  echo -e "4. Consider edge case testing for error handling paths"
fi

echo -e "\n${GREEN}Coverage analysis complete!${NC}"
exit $coverage_status