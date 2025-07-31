/// A component with a well-known name.
///
/// Users can derive this trait automatically by using the
/// [`component_name`][agent-config::component_name] macro on their structs/enums.
pub trait NamedComponent {
    /// Gets the name of the component.
    fn get_component_name(&self) -> &'static str;
}
