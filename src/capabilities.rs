#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Capability {
    RunCommand,
    GetEncoding,
    Unknown(String),
}

include!(concat!(env!("OUT_DIR"), "/capabilities.codegen.rs"));

impl<S> From<S> for Capability
where
    S: AsRef<str> + Into<String>,
{
    fn from(s: S) -> Self {
        CAPABILITIES
            .get(s.as_ref())
            .cloned()
            .unwrap_or_else(|| Capability::Unknown(s.into()))
    }
}
