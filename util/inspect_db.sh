#!/bin/bash

# inspect_db.sh - SQLite数据库检查工具
# 使用方法: ./inspect_db.sh [数据库文件路径]

set -e

# 默认数据库路径
DB_PATH="${1:-/tmp/scx_agent-2025-08-03.db}"

# 检查文件是否存在
if [[ ! -f "$DB_PATH" ]]; then
    echo "错误: 数据库文件 $DB_PATH 不存在"
    exit 1
fi

# 检查sqlite3是否安装
if ! command -v sqlite3 &> /dev/null; then
    echo "错误: sqlite3 未安装，请先安装: sudo apt install sqlite3"
    exit 1
fi

# 颜色定义
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${BLUE}SQLite数据库检查报告${NC}"
echo -e "数据库: ${YELLOW}$DB_PATH${NC}"
echo -e "大小: ${GREEN}$(ls -lh "$DB_PATH" | awk '{print $5}')${NC}"
echo -e "修改时间: ${GREEN}$(stat -c %y "$DB_PATH")${NC}"
echo -e "${GREEN}========================================${NC}"

# 获取所有表
echo -e "${BLUE}数据库中的表:${NC}"
TABLES=$(sqlite3 "$DB_PATH" "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
if [[ -z "$TABLES" ]]; then
    echo -e "${RED}  没有找到用户表${NC}"
else
    echo "$TABLES" | sed 's/^/  /'
fi

# 获取每个表的详细信息
echo ""
echo -e "${BLUE}表结构详情:${NC}"

for table in $TABLES; do
    echo ""
    echo -e "${YELLOW}表: $table${NC}"
    echo -e "${GREEN}----------------------------------------${NC}"
    
    # 表结构
    echo -e "${BLUE}结构:${NC}"
    sqlite3 "$DB_PATH" ".schema $table" | sed 's/^/  /'
    
    # 记录数量
    COUNT=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM $table;")
    echo -e "${BLUE}记录数:${NC} ${GREEN}$COUNT${NC}"
    
    # 如果表有数据，显示前10条（包含NULL值处理）
    if [[ $COUNT -gt 0 ]]; then
        echo -e "${BLUE}前10条记录:${NC}"
        
        # 获取列名
        COLUMNS=$(sqlite3 "$DB_PATH" "PRAGMA table_info($table);" | cut -d'|' -f2 | tr '\n' ',' | sed 's/,$//')
        
        # 使用NULL处理查询
        sqlite3 -header -column "$DB_PATH" "
            SELECT 
                id,
                datetime(timestamp, 'localtime') as timestamp,
                CASE 
                    WHEN data IS NULL OR data = '' THEN 'NULL'
                    ELSE substr(data, 1, 100) || CASE WHEN length(data) > 100 THEN '...' ELSE '' END
                END as data,
                CASE 
                    WHEN event_type IS NULL OR event_type = '' THEN 'NULL'
                    ELSE event_type
                END as event_type,
                CASE 
                    WHEN source IS NULL OR source = '' THEN 'NULL'
                    ELSE source
                END as source
            FROM $table 
            ORDER BY timestamp DESC 
            LIMIT 10;
        " | sed 's/^/  /'
        
        # 显示完整记录统计
        echo -e "${BLUE}完整记录统计:${NC}"
        sqlite3 "$DB_PATH" "
            SELECT 
                '总记录数: ' || COUNT(*),
                'NULL event_type: ' || SUM(CASE WHEN event_type IS NULL OR event_type = '' THEN 1 ELSE 0 END),
                'NULL source: ' || SUM(CASE WHEN source IS NULL OR source = '' THEN 1 ELSE 0 END),
                '数据大小: ' || printf('%.2f KB', SUM(LENGTH(data)) / 1024.0)
            FROM $table;
        " | sed 's/^/  /'
        
        # 事件类型分布
        echo -e "${BLUE}事件类型分布:${NC}"
        sqlite3 "$DB_PATH" "
            SELECT 
                CASE 
                    WHEN event_type IS NULL OR event_type = '' THEN 'NULL'
                    ELSE event_type
                END as event_type,
                COUNT(*) as count
            FROM $table 
            GROUP BY event_type 
            ORDER BY count DESC;
        " | sed 's/^/  /'
        
        # 如果有二进制数据，显示为十六进制
        echo -e "${BLUE}二进制字段检查:${NC}"
        HAS_BINARY=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM $table WHERE typeof(data) = 'blob' LIMIT 1;")
        if [[ $HAS_BINARY -gt 0 ]]; then
            echo -e "${YELLOW}  发现二进制数据:${NC}"
            sqlite3 "$DB_PATH" "
                SELECT 
                    id,
                    '二进制数据长度: ' || LENGTH(data) || ' bytes',
                    '十六进制: ' || quote(data)
                FROM $table 
                WHERE typeof(data) = 'blob' 
                LIMIT 3;
            " | sed 's/^/    /'
        else
            echo -e "${GREEN}  无二进制数据${NC}"
        fi
    else
        echo -e "${RED}  表中没有数据${NC}"
    fi
done

# 数据库统计信息
echo ""
echo -e "${BLUE}数据库统计:${NC}"
sqlite3 "$DB_PATH" "
    SELECT 
        '总大小: ' || printf('%.2f MB', page_count * page_size / 1024.0 / 1024.0),
        '总页数: ' || page_count,
        '空闲页数: ' || freelist_count
    FROM pragma_page_count(), pragma_freelist_count();
" 2>/dev/null | sed 's/^/  /' || echo -e "${RED}  统计信息不可用${NC}"

# 索引信息
echo ""
echo -e "${BLUE}索引信息:${NC}"
INDEXES=$(sqlite3 "$DB_PATH" "SELECT name, tbl_name, sql FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%';")
if [[ -z "$INDEXES" ]]; then
    echo -e "${RED}  没有找到索引${NC}"
else
    echo "$INDEXES" | while IFS='|' read -r index table sql; do
        echo -e "${GREEN}  索引: $index${NC} (表: ${YELLOW}$table${NC})"
        echo -e "    SQL: $sql"
    done
fi

# 数据库完整性检查
echo ""
echo -e "${BLUE}数据库完整性检查:${NC}"
if sqlite3 "$DB_PATH" "PRAGMA integrity_check;" | grep -q "ok"; then
    echo -e "${GREEN}  数据库完整性正常${NC}"
else
    echo -e "${RED}  数据库完整性有问题${NC}"
fi

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}检查完成！${NC}"
