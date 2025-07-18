// #[cfg(feature = "api")]
// use async_graphql::{SimpleObject, Union};
/* ​**#[cfg_attr]**​：
当 api feature 启用时，自动为 Matched 派生 SimpleObject trait（来自 async_graphql），使其成为 GraphQL 对象类型。
​效果​：
启用 api 时：Matched 会暴露为 GraphQL 类型，字段可被查询。
未启用时：仅作为普通 Rust 结构体。 */
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "api", derive(SimpleObject))]
/// A component was found that matched the provided pattern
pub struct Matched {
    #[cfg_attr(feature = "api", graphql(skip))]
    message: String,
    /// Pattern that raised the notification
    pub pattern: String,
}

impl Matched {
    pub fn new(pattern: String) -> Self {
        Self {
            message: format!("[tap] Pattern '{}' successfully matched.", pattern),
            pattern,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "api", derive(SimpleObject))]
/// There isn't currently a component that matches this pattern
pub struct NotMatched {
    #[cfg_attr(feature = "api", graphql(skip))]
    message: String,
    /// Pattern that raised the notification
    pub pattern: String,
}

impl NotMatched {
    pub fn new(pattern: String) -> Self {
        Self {
            message: format!(
                "[tap] Pattern '{}' failed to match: will retry on configuration reload.",
                pattern
            ),
            pattern,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "api", derive(SimpleObject))]
/// The pattern matched source(s) which cannot be tapped for inputs or sink(s)
/// which cannot be tapped for outputs
pub struct InvalidMatch {
    #[cfg_attr(feature = "api", graphql(skip))]
    message: String,
    /// Pattern that raised the notification
    pattern: String,
    /// Any invalid matches for the pattern
    invalid_matches: Vec<String>,
}

impl InvalidMatch {
    pub fn new(message: String, pattern: String, invalid_matches: Vec<String>) -> Self {
        Self {
            message,
            pattern,
            invalid_matches,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "api", derive(Union))]
/// A specific kind of notification with additional details
pub enum Notification {
    Matched(Matched),
    NotMatched(NotMatched),
    InvalidMatch(InvalidMatch),
}

impl Notification {
    pub fn as_str(&self) -> &str {
        match self {
            Notification::Matched(n) => n.message.as_ref(),
            Notification::NotMatched(n) => n.message.as_ref(),
            Notification::InvalidMatch(n) => n.message.as_ref(),
        }
    }
}
