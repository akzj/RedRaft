#!/bin/bash

# 重复执行测试直到失败的脚本
# Usage: ./run_tests_until_failure.sh [test_name]

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 默认测试命令
DEFAULT_TEST_CMD="cargo test -- --nocapture"

# 如果提供了测试名称参数，则使用指定的测试
if [ $# -gt 0 ]; then
    TEST_CMD="cargo test $1 -- --nocapture"
    echo -e "${YELLOW}Running specific test: $1${NC}"
else
    TEST_CMD="$DEFAULT_TEST_CMD"
    echo -e "${YELLOW}Running all tests${NC}"
fi

# 计数器
ITERATION=1
START_TIME=$(date +%s)

echo -e "${GREEN}Starting continuous test execution...${NC}"
echo -e "${GREEN}Test command: $TEST_CMD${NC}"
echo -e "${GREEN}Press Ctrl+C to stop manually${NC}"
echo "=================================="

while true; do
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    HOURS=$((ELAPSED / 3600))
    MINUTES=$(((ELAPSED % 3600) / 60))
    SECONDS=$((ELAPSED % 60))
    
    echo -e "\n${YELLOW}========================================${NC}"
    echo -e "${YELLOW}Iteration #$ITERATION${NC}"
    echo -e "${YELLOW}Elapsed time: ${HOURS}h ${MINUTES}m ${SECONDS}s${NC}"
    echo -e "${YELLOW}Started at: $(date)${NC}"
    echo -e "${YELLOW}========================================${NC}"
    
    # 执行测试
    if eval "$TEST_CMD"; then
        echo -e "\n${GREEN}✓ Iteration #$ITERATION PASSED${NC}"
        ITERATION=$((ITERATION + 1))
        
        # 短暂延迟避免过度占用资源
        sleep 2
    else
        EXIT_CODE=$?
        FAIL_TIME=$(date)
        
        echo -e "\n${RED}========================================${NC}"
        echo -e "${RED}✗ TEST FAILED on iteration #$ITERATION${NC}"
        echo -e "${RED}Failed at: $FAIL_TIME${NC}"
        echo -e "${RED}Total successful iterations: $((ITERATION - 1))${NC}"
        echo -e "${RED}Total elapsed time: ${HOURS}h ${MINUTES}m ${SECONDS}s${NC}"
        echo -e "${RED}Exit code: $EXIT_CODE${NC}"
        echo -e "${RED}========================================${NC}"
        
        # 保存失败信息到日志文件
        LOG_FILE="test_failure_$(date +%Y%m%d_%H%M%S).log"
        {
            echo "Test failure report"
            echo "==================="
            echo "Failed iteration: #$ITERATION"
            echo "Failed at: $FAIL_TIME"
            echo "Total successful iterations: $((ITERATION - 1))"
            echo "Total elapsed time: ${HOURS}h ${MINUTES}m ${SECONDS}s"
            echo "Test command: $TEST_CMD"
            echo "Exit code: $EXIT_CODE"
            echo ""
            echo "Last test output:"
            echo "=================="
        } > "$LOG_FILE"
        
        echo -e "${YELLOW}Failure details saved to: $LOG_FILE${NC}"
        
        exit $EXIT_CODE
    fi
done
