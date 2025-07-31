use agent_lib::config::log_schema;
use vrl::value::Kind;
use agent_lib::schema::Requirement;
/// Inspect the global log schema and create a schema requirement.
pub fn get_serializer_schema_requirement() -> Requirement {
    if let Some(message_key) = log_schema().message_key() {
        Requirement::empty().required_meaning(message_key.to_string(), Kind::any())
    } else {
        Requirement::empty()
    }
}
