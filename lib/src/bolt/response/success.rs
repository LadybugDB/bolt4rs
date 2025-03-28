use serde::Deserialize;
use serde::Serialize;

/// Represents the metadata map often contained within a SUCCESS message,
/// particularly the one sent after a HELLO message.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Meta {
    // Fields commonly found in the SUCCESS metadata map.
    // Use `Option` or `#[serde(default)]` if fields are not always present.
    #[serde(default)]
    pub(crate) server: String,
    #[serde(default)]
    pub(crate) connection_id: String,
    // Add other potential fields if needed, e.g.:
    // pub(crate) routing: Option<HashMap<String, serde_bolt::Value>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bolt::MessageResponse; // Trait needed for `parse`
    use crate::packstream::bolt;      // For building test data

    #[test]
    fn should_deserialize_success_metadata() {
        // Construct the byte representation of the metadata map payload
        let metadata_bytes = bolt()
            .tiny_map(2)
            .tiny_string("server")
            .tiny_string("Neo4j/4.1.4")
            .tiny_string("connection_id")
            .tiny_string("bolt-31")
            .build();

        // Parse the metadata bytes using the MessageResponse trait implementation
        let success_meta = Meta::parse(metadata_bytes).unwrap();

        assert_eq!(success_meta.server, "Neo4j/4.1.4");
        assert_eq!(success_meta.connection_id, "bolt-31");
    }

    #[test]
    fn should_deserialize_empty_success_metadata() {
        // SUCCESS messages for operations like COMMIT often have an empty map
        let metadata_bytes = bolt().tiny_map(0).build();

        let success_meta= Meta::parse(metadata_bytes).unwrap();

        // Default values should be used
        assert_eq!(success_meta.server, "");
        assert_eq!(success_meta.connection_id, "");
    }
}
