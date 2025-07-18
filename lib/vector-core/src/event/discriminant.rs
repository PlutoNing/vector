use std::fmt;
use std::hash::{Hash, Hasher};

use super::{LogEvent, ObjectMap, Value};

// TODO: if we had `Value` implement `Eq` and `Hash`, the implementation here
// would be much easier. The issue is with `f64` type. We should consider using
// a newtype for `f64` there that'd implement `Eq` and `Hash` if it's safe, for
// example `NormalF64`, and guard the values with `val.is_normal() == true`
// invariant.
// See also: https://internals.rust-lang.org/t/f32-f64-should-implement-hash/5436/32

/// An event discriminant identifies a distinguishable subset of events.
/// Intended for dissecting streams of events to sub-streams, for instance to
/// be able to allocate a buffer per sub-stream.
/// Implements `PartialEq`, `Eq` and `Hash` to enable use as a `HashMap` key.
#[derive(Debug, Clone)]
pub struct Discriminant {
    values: Vec<Option<Value>>,
}

impl Discriminant {
    /// Create a new Discriminant from the `LogEvent` and an ordered slice of
    /// fields to include into a discriminant value.
    pub fn from_log_event(event: &LogEvent, discriminant_fields: &[impl AsRef<str>]) -> Self {
        let values: Vec<Option<Value>> = discriminant_fields
            .iter()
            .map(|discriminant_field| {
                event
                    .parse_path_and_get_value(discriminant_field.as_ref())
                    .ok()
                    .flatten()
                    .cloned()
            })
            .collect();
        Self { values }
    }
}

impl PartialEq for Discriminant {
    fn eq(&self, other: &Self) -> bool {
        self.values
            .iter()
            .zip(other.values.iter())
            .all(|(this, other)| match (this, other) {
                (None, None) => true,
                (Some(this), Some(other)) => value_eq(this, other),
                _ => false,
            })
    }
}

impl Eq for Discriminant {}

// Equality check for discriminant purposes.
fn value_eq(this: &Value, other: &Value) -> bool {
    match (this, other) {
        // Trivial.
        (Value::Bytes(this), Value::Bytes(other)) => this.eq(other),
        (Value::Boolean(this), Value::Boolean(other)) => this.eq(other),
        (Value::Integer(this), Value::Integer(other)) => this.eq(other),
        (Value::Timestamp(this), Value::Timestamp(other)) => this.eq(other),
        (Value::Null, Value::Null) => true,
        // Non-trivial.
        (Value::Float(this), Value::Float(other)) => f64_eq(this.into_inner(), other.into_inner()),
        (Value::Array(this), Value::Array(other)) => array_eq(this, other),
        (Value::Object(this), Value::Object(other)) => map_eq(this, other),
        // Type mismatch.
        _ => false,
    }
}

// Does an f64 comparison that is suitable for discriminant purposes.
fn f64_eq(this: f64, other: f64) -> bool {
    if this.is_nan() && other.is_nan() {
        return true;
    }
    if this != other {
        return false;
    }
    if (this.is_sign_positive() && other.is_sign_negative())
        || (this.is_sign_negative() && other.is_sign_positive())
    {
        return false;
    }
    true
}

fn array_eq(this: &[Value], other: &[Value]) -> bool {
    if this.len() != other.len() {
        return false;
    }

    this.iter()
        .zip(other.iter())
        .all(|(first, second)| value_eq(first, second))
}

fn map_eq(this: &ObjectMap, other: &ObjectMap) -> bool {
    if this.len() != other.len() {
        return false;
    }

    this.iter()
        .zip(other.iter())
        .all(|((key1, value1), (key2, value2))| key1 == key2 && value_eq(value1, value2))
}

impl Hash for Discriminant {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for value in &self.values {
            match value {
                Some(value) => {
                    state.write_u8(1);
                    hash_value(state, value);
                }
                None => state.write_u8(0),
            }
        }
    }
}

// Hashes value for discriminant purposes.
fn hash_value<H: Hasher>(hasher: &mut H, value: &Value) {
    match value {
        // Trivial.
        Value::Bytes(val) => val.hash(hasher),
        Value::Regex(val) => val.as_bytes_slice().hash(hasher),
        Value::Boolean(val) => val.hash(hasher),
        Value::Integer(val) => val.hash(hasher),
        Value::Timestamp(val) => val.hash(hasher),
        // Non-trivial.
        Value::Float(val) => hash_f64(hasher, val.into_inner()),
        Value::Array(val) => hash_array(hasher, val),
        Value::Object(val) => hash_map(hasher, val),
        Value::Null => hash_null(hasher),
    }
}

// Does f64 hashing that is suitable for discriminant purposes.
fn hash_f64<H: Hasher>(hasher: &mut H, value: f64) {
    hasher.write(&value.to_ne_bytes());
}

fn hash_array<H: Hasher>(hasher: &mut H, array: &[Value]) {
    for val in array {
        hash_value(hasher, val);
    }
}

fn hash_map<H: Hasher>(hasher: &mut H, map: &ObjectMap) {
    for (key, val) in map {
        hasher.write(key.as_bytes());
        hash_value(hasher, val);
    }
}

fn hash_null<H: Hasher>(hasher: &mut H) {
    hasher.write_u8(0);
}

impl fmt::Display for Discriminant {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        for (i, value) in self.values.iter().enumerate() {
            if i != 0 {
                write!(fmt, "-")?;
            }
            if let Some(value) = value {
                value.fmt(fmt)?;
            } else {
                fmt.write_str("none")?;
            }
        }
        Ok(())
    }
}