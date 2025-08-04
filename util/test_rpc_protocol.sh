#!/bin/bash

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 配置
SERVER_BIN="./target/debug/grpc-server"
CMD_BIN="./target/debug/grpc-cmd"
SERVER_LOG="/tmp/grpc-server-test.log"
SOCKET_PATH="/tmp/scx_agent/agent_rpc.sock"

# 测试用例
TEST_CASES=(
    "15 25"
    "100 200"
)

# 清理函数
cleanup() {
    echo -e "${YELLOW}清理环境...${NC}"
    
    # 停止server
    if [[ -n "$SERVER_PID" ]] && kill -0 $SERVER_PID 2>/dev/null; then
        echo -e "${YELLOW}停止server (PID: $SERVER_PID)...${NC}"
        kill $SERVER_PID 2>/dev/null
        wait $SERVER_PID 2>/dev/null
    fi
    
    if [[ -e "$SOCKET_PATH" ]]; then
        echo -e "${YELLOW}清理socket文件...${NC}"
        rm -f "$SOCKET_PATH"
    fi
    
    rm -f "$SERVER_LOG"
}

trap cleanup EXIT

# 检查文件是否存在
check_files() {
    echo -e "${BLUE}检查bin...${NC}"
    
    if [[ ! -f "$SERVER_BIN" ]]; then
        echo -e "${RED}Server不存在: $SERVER_BIN${NC}"
        exit 1
    fi
    
    if [[ ! -f "$CMD_BIN" ]]; then
        echo -e "${RED}Client文件不存在: $CMD_BIN${NC}"
        exit 1
    fi
}

# 启动server
start_server() {
    echo -e "${BLUE}启动gRPC server...${NC}"
    
    # 清理旧的日志
    rm -f "$SERVER_LOG"
    
    # 启动server并将输出重定向到日志文件
    $SERVER_BIN > "$SERVER_LOG" 2>&1 &
    SERVER_PID=$!
    
    echo -e "${BLUE}Server PID: $SERVER_PID${NC}"
    
    # 等待server启动
    local retries=0
    while [[ $retries -lt 10 ]]; do
        if [[ -e "$SOCKET_PATH" ]]; then
            echo -e "${GREEN}Server启动成功${NC}"
            return 0
        fi
        sleep 1
        ((retries++))
    done
    
    echo -e "${RED}Server启动失败${NC}"
    echo -e "${RED}Server日志内容:${NC}"
    cat "$SERVER_LOG"
    exit 1
}

run_tests() {
    echo -e "${BLUE}开始运行测试...${NC}"
    
    local total_tests=${#TEST_CASES[@]}
    local passed_tests=0
    
    # 先检查命令用法
    echo -e "${BLUE}检查命令用法...${NC}"
    $CMD_BIN --help 2>&1 || echo "命令无help选项"
    
    for i in "${!TEST_CASES[@]}"; do
        local test_case="${TEST_CASES[$i]}"
        local a=$(echo $test_case | cut -d' ' -f1)
        local b=$(echo $test_case | cut -d' ' -f2)
        local expected=$((a + b))
        
        echo -e "\n${BLUE}开始测试${NC}"
        echo -e "${BLUE}测试输入: $a + $b${NC}"
        echo -e "${BLUE}期望结果: $expected${NC}"
        echo -e "${BLUE}执行命令: $CMD_BIN $a $b${NC}"
        
        local output=$($CMD_BIN $a $b 2>&1)
        local exit_code=$?
        
        if [[ $exit_code -eq 0 ]]; then
            echo -e "${GREEN}测试通过${NC}"
            echo -e "${GREEN}$output${NC}"
            ((passed_tests++))
        else
            echo -e "${RED}测试失败${NC}"
            echo -e "${RED} $output${NC}"
        fi
    done
}

# 显示server日志
show_server_log() {
    echo -e "\n${BLUE}Server运行日志:${NC}"
    if [[ -f "$SERVER_LOG" ]]; then
        cat "$SERVER_LOG"
    else
        echo -e "${YELLOW}无日志文件${NC}"
    fi
}

# 主函数
main() {
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}  测试脚本${NC}"
    echo -e "${GREEN}========================================${NC}"
    
    check_files
    start_server
    
    # 可选：实时查看server日志（在后台）
    tail -f "$SERVER_LOG" &
    TAIL_PID=$!
    
    run_tests
    
    # 停止tail
    kill $TAIL_PID 2>/dev/null
    
    # show_server_log
    
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}  测试完成${NC}"
    echo -e "${GREEN}========================================${NC}"
}

# 运行主函数
main
