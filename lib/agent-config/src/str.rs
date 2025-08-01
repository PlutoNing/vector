use vrl::value::KeyString;

use crate::Configurable;

/// 可以在配置中表示的字符串类型。
/// 因为映射在表示为对象时只能有字符串类型的键。
/// 如果为非字符串类型实现了这个 trait，很可能会导致问题。
pub trait ConfigurableString: Configurable + ToString {}

impl ConfigurableString for String {}

impl ConfigurableString for KeyString {}
