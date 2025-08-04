use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(name = "query")]
#[command(about = "SQLite数据采集查询工具")]
#[command(version = "1.0")]
pub struct QueryCli {
    #[command(subcommand)]
    pub command: QuerySubCommands,
}


#[derive(Subcommand)]
pub enum QuerySubCommands {
    /// 获取事件数据记录
    Get(QueryGetArgs),
    /// 统计数据摘要信息
    Stats(QueryStatsArgs),
    /// 搜索事件内容
    Search(QuerySearchArgs),
}

#[derive(Args)]
pub struct QueryGetArgs {
    /// 返回记录数量限制
    #[arg(short, long, default_value = "100")]
    limit: usize,
    
    /// 结果偏移量（分页）
    #[arg(short, long, default_value = "0")]
    offset: usize,
    
    /// 事件类型过滤
    #[arg(short, long)]
    event_type: Option<String>,
    
    /// 数据来源过滤
    #[arg(short, long)]
    source: Option<String>,
    
    /// 相对时间范围
    #[arg(long)]
    since: Option<String>,
    
    /// 绝对开始时间
    #[arg(long)]
    start_time: Option<String>,
    
    /// 绝对结束时间
    #[arg(long)]
    end_time: Option<String>,
    
    /// 输出格式选择
    #[arg(long, value_enum, default_value = "table")]
    format: QueryOutputFormat,
    
    /// 指定返回字段
    #[arg(long, value_delimiter = ',')]
    fields: Option<Vec<String>>,
}
#[derive(Args)]
pub struct QueryStatsArgs {
    /// 保留扩展
    #[arg(skip)]
    _placeholder: (),
}

#[derive(Args)]
pub struct QuerySearchArgs {
    /// 保留扩展
    #[arg(skip)]
    _placeholder: (),
}

#[derive(ValueEnum, Clone)]
pub enum QueryOutputFormat {
    Table,
    Json,
    Csv,
    Yaml,
}