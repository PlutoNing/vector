#![deny(warnings)]

// Re-export of the various public dependencies required by the generated code to simplify the import requirements for
// crates actually using the macros/derives.
pub mod indexmap {
    pub use indexmap::*;
}

pub use serde_json;

pub mod component;
mod configurable;
pub use self::configurable::{Configurable, ConfigurableRef, ToValue};
mod errors;
pub use self::errors::{BoundDirection, GenerateError};
mod external;
mod metadata;
pub use self::metadata::Metadata;
mod named;
pub use self::named::NamedComponent;
mod num;
pub use self::num::{ConfigurableNumber, NumberClass};
pub mod schema;
pub mod ser;
mod stdlib;
mod str;
pub use self::str::ConfigurableString;

// Re-export of the `#[configurable_component]` and `#[derive(Configurable)]` proc macros.
pub use agent_config_macros::*;

// Re-export of both `Format` and `Validation` from `agent_config_common`.
//
// The crate exists so that both `agent_config_macros` and `agent_config` can import the types and work with them
// natively, but from a codegen and usage perspective, it's much cleaner to export everything needed to use
// `Configurable` from `agent_config` itself, and not leak out the crate arrangement as an impl detail.
pub use agent_config_common::{attributes, validation};

#[doc(hidden)]
pub fn __ensure_numeric_validation_bounds<N>(metadata: &Metadata) -> Result<(), GenerateError>
where
    N: Configurable + ConfigurableNumber,
{
    // In `Validation::ensure_conformance`, we do some checks on any supplied numeric bounds to try and ensure they're
    // no larger than the largest f64 value where integer/floating-point conversions are still lossless.  What we
    // cannot do there, however, is ensure that the bounds make sense for the type on the Rust side, such as a user
    // supplying a negative bound which would be fine for `i64`/`f64` but not for `u64`. That's where this function
    // comes in.
    //
    // We simply check the given metadata for any numeric validation bounds, and ensure they do not exceed the
    // mechanical limits of the given numeric type `N`.  If they do, we panic, which is not as friendly as a contextual
    // Compile-time error emitted from the `Configurable` derive macro... but we're working with what we've got here.
    let mechanical_min_bound = N::get_enforced_min_bound();
    let mechanical_max_bound = N::get_enforced_max_bound();

    for validation in metadata.validations() {
        if let validation::Validation::Range { minimum, maximum } = validation {
            if let Some(min_bound) = minimum {
                if *min_bound < mechanical_min_bound {
                    return Err(GenerateError::IncompatibleNumericBounds {
                        numeric_type: std::any::type_name::<N>(),
                        bound_direction: BoundDirection::Minimum,
                        mechanical_bound: mechanical_min_bound,
                        specified_bound: *min_bound,
                    });
                }
            }

            if let Some(max_bound) = maximum {
                if *max_bound > mechanical_max_bound {
                    return Err(GenerateError::IncompatibleNumericBounds {
                        numeric_type: std::any::type_name::<N>(),
                        bound_direction: BoundDirection::Maximum,
                        mechanical_bound: mechanical_max_bound,
                        specified_bound: *max_bound,
                    });
                }
            }
        }
    }

    Ok(())
}
