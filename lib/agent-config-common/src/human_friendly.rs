use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use convert_case::{Boundary, Case, Converter};

/// Well-known replacements.
///
/// Replacements are instances of strings with unique capitalization that cannot be achieved
/// programmatically, as well as the potential insertion of additional characters, such as the
/// replacement of "pubsub" with "Pub/Sub".
static WELL_KNOWN_REPLACEMENTS: LazyLock<HashMap<String, &'static str>> = LazyLock::new(|| {
    let pairs = vec![
        ("eventstoredb", "EventStoreDB"),
        ("mongodb", "MongoDB"),
        ("opentelemetry", "OpenTelemetry"),
        ("otel", "OTEL"),
        ("postgresql", "PostgreSQL"),
        ("pubsub", "Pub/Sub"),
        ("statsd", "StatsD"),
        ("journald", "JournalD"),
        ("appsignal", "AppSignal"),
        ("webhdfs", "WebHDFS"),
        ("cloudwatch", "CloudWatch"),
        ("logdna", "LogDNA"),
        ("geoip", "GeoIP"),
        ("ssekms", "SSE-KMS"),
        ("aes256", "AES-256"),
        ("apiserver", "API Server"),
        ("dir", "Directory"),
        ("ids", "IDs"),
        ("ips", "IPs"),
        ("oauth2", "OAuth2"),
    ];

    pairs.iter().map(|(k, v)| (k.to_lowercase(), *v)).collect()
});

/// Well-known acronyms.
///
/// Acronyms are distinct from replacements because they should be entirely capitalized (i.e. "aws"
/// or "aWs" or "Aws" should always be replaced with "AWS") whereas replacements may insert
/// additional characters or capitalize specific characters within the original string.
static WELL_KNOWN_ACRONYMS: LazyLock<HashSet<String>> = LazyLock::new(|| {
    let acronyms = &[
        "api", "aws", "ec2", "ecs", "hec", "http", "https", "nginx", "s3",
        "sqs", "tls", "ssl", "otel", "json", "rfc3339", "lz4", "us", "eu", "bsd",
        "vrl", "tcp", "udp", "id", "uuid", "kms", "uri", "url", "acp", "uid", "ip", "pid",
        "ndjson", "ewma", "rtt", "cpu", "acl", "imds", "acl", "alpn", "sasl",
    ];

    acronyms.iter().map(|s| s.to_lowercase()).collect()
});

/// Generates a human-friendly version of the given string.
///
/// Many instances exist where type names, or string constants, represent a condensed form of an
/// otherwise human-friendly/recognize string, such as "aws_s3" (for AWS S3) or "InfluxdbMetrics"
/// (for InfluxDB Metrics) and so on.
///
/// This function takes a given input and restores it back to the human-friendly version by
/// splitting it on the relevant word boundaries, adjusting the input to title case, and applying
/// well-known replacements to ensure that brand-specific casing (such as "CloudWatch" instead of
/// "Cloudwatch", or handling acronyms like AWS, GCP, and so on) makes it into the final version.
pub fn generate_human_friendly_string(input: &str) -> String {
    // Create our case converter, which specifically ignores letter/digit boundaries, which is
    // important for not turning substrings like "Ec2" or "S3" into "Ec"/"2" and "S"/"3",
    // respectively.
    let converter = Converter::new()
        .to_case(Case::Title)
        .remove_boundaries(&[Boundary::LOWER_DIGIT, Boundary::UPPER_DIGIT]);
    let normalized = converter.convert(input);

    let replaced_segments = normalized
        .split(' ')
        .map(replace_well_known_segments)
        .collect::<Vec<_>>();
    replaced_segments.join(" ")
}

fn replace_well_known_segments(input: &str) -> String {
    let as_lower = input.to_lowercase();
    if let Some(replacement) = WELL_KNOWN_REPLACEMENTS.get(&as_lower) {
        replacement.to_string()
    } else if WELL_KNOWN_ACRONYMS.contains(&as_lower) {
        input.to_uppercase()
    } else {
        input.to_string()
    }
}