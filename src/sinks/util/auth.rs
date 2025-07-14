#[derive(Debug, Clone)]
pub enum Auth {
    Basic(crate::http::Auth),
}
