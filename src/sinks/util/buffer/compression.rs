use std::{cell::RefCell, collections::BTreeSet, fmt};

use indexmap::IndexMap;
use serde::{de, ser};
use serde_json::Value;
use vector_lib::configurable::attributes::CustomAttribute;
use vector_lib::configurable::{
    schema::{
        apply_base_metadata, generate_const_string_schema, generate_enum_schema,
        generate_one_of_schema, generate_struct_schema, get_or_generate_schema, SchemaGenerator,
        SchemaObject,
    },
    Configurable, GenerateError, Metadata, ToValue,
};

/// Compression configuration.
#[derive(Copy, Clone, Debug, Derivative, Eq, PartialEq)]
#[derivative(Default)]
pub enum Compression {
    /// No compression.
    #[derivative(Default)]
    None,

    /// [Gzip][gzip] compression.
    ///
    /// [gzip]: https://www.gzip.org/
    Gzip(CompressionLevel),
}

impl Compression {
    /// Gets whether or not this compression will actually compress the input.
    ///
    /// While it may be counterintuitive for "compression" to not compress, this is simply a
    /// consequence of designing a single type that may or may not compress so that we can avoid
    /// having to box writers at a higher-level.
    ///
    /// Some callers can benefit from knowing whether or not compression is actually taking place,
    /// as different size limitations may come into play.
    pub const fn is_compressed(&self) -> bool {
        !matches!(self, Compression::None)
    }

    pub const fn gzip_default() -> Compression {
        Compression::Gzip(CompressionLevel::const_default())
    }

    pub const fn content_encoding(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Gzip(_) => Some("gzip"),
        }
    }

    pub const fn accept_encoding(self) -> Option<&'static str> {
        match self {
            Self::Gzip(_) => Some("gzip"),
            _ => None,
        }
    }

    pub const fn extension(self) -> &'static str {
        match self {
            Self::None => "log",
            Self::Gzip(_) => "log.gz",
        }
    }

    pub const fn max_compression_level_val(self) -> u32 {
        match self {
            Compression::None => 0,
            Compression::Gzip(_) => 9,
        }
    }

    pub const fn compression_level(self) -> CompressionLevel {
        match self {
            Self::None => CompressionLevel::None,
            Self::Gzip(level) => level,
        }
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Compression::None => write!(f, "none"),
            Compression::Gzip(ref level) => write!(f, "gzip({})", level.as_flate2().level()),
        }
    }
}

impl<'de> de::Deserialize<'de> for Compression {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct StringOrMap;

        impl<'de> de::Visitor<'de> for StringOrMap {
            type Value = Compression;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("string or map")
            }

            fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match s {
                    "none" => Ok(Compression::None),
                    "gzip" => Ok(Compression::gzip_default()),
                    _ => Err(de::Error::invalid_value(
                        de::Unexpected::Str(s),
                        &r#""none" or "gzip" or "zstd""#,
                    )),
                }
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut algorithm = None;
                let mut level = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "algorithm" => {
                            if algorithm.is_some() {
                                return Err(de::Error::duplicate_field("algorithm"));
                            }
                            algorithm = Some(map.next_value::<String>()?);
                        }
                        "level" => {
                            if level.is_some() {
                                return Err(de::Error::duplicate_field("level"));
                            }
                            level = Some(map.next_value::<CompressionLevel>()?);
                        }
                        _ => return Err(de::Error::unknown_field(&key, &["algorithm", "level"])),
                    };
                }

                let compression = match algorithm
                    .ok_or_else(|| de::Error::missing_field("algorithm"))?
                    .as_str()
                {
                    "none" => match level {
                        Some(_) => Err(de::Error::unknown_field("level", &[])),
                        None => Ok(Compression::None),
                    },
                    "gzip" => Ok(Compression::Gzip(level.unwrap_or_default())),
                    algorithm => Err(de::Error::unknown_variant(algorithm, &["none", "gzip"])),
                }?;

                if let CompressionLevel::Val(level) = compression.compression_level() {
                    let max_level = compression.max_compression_level_val();
                    if level > max_level {
                        let msg = std::format!(
                            "invalid value `{}`, expected value in range [0, {}]",
                            level,
                            max_level
                        );
                        return Err(de::Error::custom(msg));
                    }
                }

                Ok(compression)
            }
        }

        deserializer.deserialize_any(StringOrMap)
    }
}

impl ser::Serialize for Compression {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        use ser::SerializeMap;

        match self {
            Compression::None => serializer.serialize_str("none"),
            Compression::Gzip(gzip_level) => {
                if *gzip_level != CompressionLevel::Default {
                    let mut map = serializer.serialize_map(None)?;
                    map.serialize_entry("algorithm", "gzip")?;
                    map.serialize_entry("level", &gzip_level)?;
                    map.end()
                } else {
                    serializer.serialize_str("gzip")
                }
            }
        }
    }
}

pub const ALGORITHM_NAME: &str = "algorithm";
pub const LEVEL_NAME: &str = "level";
pub const LOGICAL_NAME: &str = "logical_name";
pub const ENUM_TAGGING_MODE: &str = "docs::enum_tagging";

pub fn generate_string_schema(
    logical_name: &str,
    title: Option<&'static str>,
    description: &'static str,
) -> SchemaObject {
    let mut const_schema = generate_const_string_schema(logical_name.to_lowercase());
    let mut const_metadata = Metadata::with_description(description);
    if let Some(title) = title {
        const_metadata.set_title(title);
    }
    const_metadata.add_custom_attribute(CustomAttribute::kv(LOGICAL_NAME, logical_name));
    apply_base_metadata(&mut const_schema, const_metadata);
    const_schema
}

// TODO: Consider an approach for generating schema of "string or object" structure used by this type.
impl Configurable for Compression {
    fn referenceable_name() -> Option<&'static str> {
        Some(std::any::type_name::<Self>())
    }

    fn metadata() -> Metadata {
        let mut metadata = Metadata::default();
        metadata.set_title("Compression configuration.");
        metadata.set_description("All compression algorithms use the default compression level unless otherwise specified.");
        metadata.add_custom_attribute(CustomAttribute::kv("docs::enum_tagging", "external"));
        metadata.add_custom_attribute(CustomAttribute::flag("docs::advanced"));
        metadata
    }

    fn generate_schema(gen: &RefCell<SchemaGenerator>) -> Result<SchemaObject, GenerateError> {
        // First, we'll create the string-only subschemas for each algorithm, and wrap those up
        // within a one-of schema.
        let mut string_metadata = Metadata::with_description("Compression algorithm.");
        string_metadata.add_custom_attribute(CustomAttribute::kv(ENUM_TAGGING_MODE, "external"));

        let none_string_subschema = generate_string_schema("None", None, "No compression.");
        let gzip_string_subschema = generate_string_schema(
            "Gzip",
            Some("[Gzip][gzip] compression."),
            "[gzip]: https://www.gzip.org/",
        );

        let mut all_string_oneof_subschema =
            generate_one_of_schema(&[none_string_subschema, gzip_string_subschema]);
        apply_base_metadata(&mut all_string_oneof_subschema, string_metadata);

        // Next we'll create a full schema for the given algorithms.
        //
        // TODO: We're currently using all three algorithms in the enum subschema for `algorithm`,
        // but in reality, `level` is never used when the algorithm is `none`. This is _currently_
        // fine because the field is optional, and we don't use `deny_unknown_fields`, so if users
        // specify it when the algorithm is `none`: no harm, no foul.
        //
        // However, it does lead to a suboptimal schema being generated, one that sort of implies it
        // may have value when set, even if the algorithm is `none`. We do this because, otherwise,
        // it's very hard to reconcile the resolved schemas during component documentation
        // generation, where we need to be able to generate the right enum key/value pair for the
        // `none` algorithm as part of the overall set of enum values declared for the `algorithm`
        // field in the "full" schema version.
        let compression_level_schema =
            get_or_generate_schema(&CompressionLevel::as_configurable_ref(), gen, None)?;

        let mut required = BTreeSet::new();
        required.insert(ALGORITHM_NAME.to_string());

        let mut properties = IndexMap::new();
        properties.insert(
            ALGORITHM_NAME.to_string(),
            all_string_oneof_subschema.clone(),
        );
        properties.insert(LEVEL_NAME.to_string(), compression_level_schema);

        let mut full_subschema = generate_struct_schema(properties, required, None);
        let mut full_metadata =
            Metadata::with_description("Compression algorithm and compression level.");
        full_metadata.add_custom_attribute(CustomAttribute::flag("docs::hidden"));
        apply_base_metadata(&mut full_subschema, full_metadata);

        // Finally, we zip both schemas together.
        Ok(generate_one_of_schema(&[
            all_string_oneof_subschema,
            full_subschema,
        ]))
    }
}

impl ToValue for Compression {
    fn to_value(&self) -> Value {
        serde_json::to_value(self).expect("Could not convert compression settings to JSON")
    }
}

/// Compression level.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CompressionLevel {
    None,
    #[default]
    Default,
    Best,
    Fast,
    Val(u32),
}

impl CompressionLevel {
    pub const fn const_default() -> Self {
        CompressionLevel::Default
    }

    pub fn as_flate2(self) -> flate2::Compression {
        match self {
            CompressionLevel::None => flate2::Compression::none(),
            CompressionLevel::Default => flate2::Compression::default(),
            CompressionLevel::Best => flate2::Compression::best(),
            CompressionLevel::Fast => flate2::Compression::fast(),
            CompressionLevel::Val(level) => flate2::Compression::new(level),
        }
    }
}

impl<'de> de::Deserialize<'de> for CompressionLevel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct NumberOrString;

        impl de::Visitor<'_> for NumberOrString {
            type Value = CompressionLevel;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("unsigned number or string")
            }

            fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match s {
                    "none" => Ok(CompressionLevel::None),
                    "fast" => Ok(CompressionLevel::Fast),
                    "default" => Ok(CompressionLevel::Default),
                    "best" => Ok(CompressionLevel::Best),
                    level => Err(de::Error::invalid_value(
                        de::Unexpected::Str(level),
                        &r#""none", "fast", "best" or "default""#,
                    )),
                }
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                u32::try_from(v).map(CompressionLevel::Val).map_err(|err| {
                    de::Error::custom(format!(
                        "unsigned integer could not be converted to u32: {}",
                        err
                    ))
                })
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                u32::try_from(v).map(CompressionLevel::Val).map_err(|err| {
                    de::Error::custom(format!("integer could not be converted to u32: {}", err))
                })
            }
        }

        deserializer.deserialize_any(NumberOrString)
    }
}

impl ser::Serialize for CompressionLevel {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match *self {
            CompressionLevel::None => serializer.serialize_str("none"),
            CompressionLevel::Default => serializer.serialize_str("default"),
            CompressionLevel::Best => serializer.serialize_str("best"),
            CompressionLevel::Fast => serializer.serialize_str("fast"),
            CompressionLevel::Val(level) => serializer.serialize_u64(u64::from(level)),
        }
    }
}

// TODO: Consider an approach for generating schema of "string or number" structure used by this type.
impl Configurable for CompressionLevel {
    fn referenceable_name() -> Option<&'static str> {
        Some(std::any::type_name::<Self>())
    }

    fn metadata() -> Metadata {
        let mut metadata = Metadata::default();
        metadata.set_description("Compression level.");
        metadata
    }

    fn generate_schema(_: &RefCell<SchemaGenerator>) -> Result<SchemaObject, GenerateError> {
        let string_consts = ["none", "fast", "best", "default"]
            .iter()
            .map(|s| serde_json::Value::from(*s));

        let level_consts = (0u32..=21).map(serde_json::Value::from);

        let valid_values = string_consts.chain(level_consts).collect();
        Ok(generate_enum_schema(valid_values))
    }
}

impl ToValue for CompressionLevel {
    fn to_value(&self) -> Value {
        // FIXME
        serde_json::to_value(self).expect("Could not convert compression level to JSON")
    }
}
