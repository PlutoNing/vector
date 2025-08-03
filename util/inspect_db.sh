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

echo "========================================"
echo "SQLite数据库检查报告"
echo "数据库: $DB_PATH"
echo "大小: $(ls -lh "$DB_PATH" | awk '{print $5}')"
echo "修改时间: $(stat -c %y "$DB_PATH")"
echo "========================================"

# 获取所有表
echo "数据库中的表:"
sqlite3 "$DB_PATH" ".tables" | sed 's/^/  /'

# 获取每个表的详细信息
echo ""
echo "表结构详情:"

# 获取所有表名
TABLES=$(sqlite3 "$DB_PATH" "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")

for table in $TABLES; do
    echo ""
    echo "表: $table"
    echo "----------------------------------------"
    
    # 表结构
    echo "结构:"
    sqlite3 "$DB_PATH" ".schema $table" | sed 's/^/  /'
    
    # 记录数量
    COUNT=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM $table;")
    echo "记录数: $COUNT"
    
    # 如果表有数据，显示前5条
    if [[ $COUNT -gt 0 ]]; then
        echo "前5条记录:"
        sqlite3 "$DB_PATH" "SELECT * FROM $table LIMIT 5;" | sed 's/^/  /'
        
        # 如果有二进制字段，显示为十六进制
        echo "二进制字段检查:"
        COLUMNS=$(sqlite3 "$DB_PATH" "PRAGMA table_info($table);" | cut -d'|' -f2)
        
        for col in $COLUMNS; do
            # 检查是否有二进制数据
            HAS_BINARY=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM $table WHERE typeof($col) = 'blob' LIMIT 1;")
            if [[ $HAS_BINARY -gt 0 ]]; then
                echo "  字段 '$col' 包含二进制数据:"
                sqlite3 "$DB_PATH" "SELECT quote($col) FROM $table WHERE typeof($col) = 'blob' LIMIT 3;" | sed 's/^/    /'
            fi
        done
    fi
done

# 数据库统计信息
echo ""
echo "数据库统计:"
sqlite3 "$DB_PATH" "SELECT '总页数: ' || page_count FROM dbstat WHERE name='sqlite_master';" 2>/dev/null || echo "  统计信息不可用"

# 索引信息
echo ""
echo "索引信息:"
sqlite3 "$DB_PATH" "SELECT name, tbl_name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%';" | while read -r index table; do
    echo "  索引: $index (表: $table)"
done

echo ""
echo "========================================"
echo "检查完成！"
