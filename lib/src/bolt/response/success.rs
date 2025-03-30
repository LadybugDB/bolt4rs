use rustls::crypto::hash::Hash;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::vec;

use crate::{
    types::{BoltMap, BoltString},
    BoltType,
};

/// Represents the metadata map often contained within a SUCCESS message,
/// particularly the one sent after a HELLO message.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Meta {
    #[serde(default)]
    pub(crate) server: String,
    #[serde(default)]
    pub(crate) connection_id: String,
    #[serde(default = "default_true")]
    pub(crate) done: bool,
    #[serde(default = "default_false")]
    pub(crate) has_more: bool,
    #[serde(default)]
    pub(crate) fields: Vec<String>,
}

fn default_true() -> bool {
    true
}

fn default_false() -> bool {
    false
}

/// Builder for creating Meta instances with default values.
pub struct MetaBuilder {
    server: String,
    connection_id: String,
    fields: Vec<String>,
    done: bool,
    has_more: bool,
}

impl MetaBuilder {
    /// Creates a new MetaBuilder with empty string values for server and connection_id,
    /// and defaults done to true and has_more to false.
    pub fn new() -> Self {
        Self {
            server: String::new(),
            connection_id: String::new(),
            fields: vec![],
            done: true,
            has_more: false,
        }
    }

    /// Sets the server value.
    pub fn server(mut self, server: impl Into<String>) -> Self {
        self.server = server.into();
        self
    }

    /// Sets the connection_id value.
    pub fn connection_id(mut self, connection_id: impl Into<String>) -> Self {
        self.connection_id = connection_id.into();
        self
    }

    /// Sets the done value.
    pub fn done(mut self, done: bool) -> Self {
        self.done = done;
        self
    }

    /// Sets the has_more value.
    pub fn has_more(mut self, has_more: bool) -> Self {
        self.has_more = has_more;
        self
    }

    /// Adds a custom field with the provided key and value.
    pub fn field<K>(mut self, key: K) -> Self
    where
        K: Into<String>,
    {
        self.fields.push(key.into());
        self
    }

    /// Create one from an iterator
    pub fn fields<K>(mut self, fields: impl IntoIterator<Item = K>) -> Self
    where
        K: Into<String>,
    {
        for key in fields {
            self.fields.push(key.into());
        }
        self
    }

    /// Builds the Meta instance.
    pub fn build(self) -> Meta {
        Meta {
            server: self.server,
            connection_id: self.connection_id,
            done: self.done,
            has_more: self.has_more,
            fields: self.fields,
        }
    }
}

impl Default for MetaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bolt::MessageResponse; // Trait needed for `parse`
    use crate::packstream::bolt; // For building test data
    use crate::types::{BoltFloat, BoltInteger, BoltString};

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

        let success_meta = Meta::parse(metadata_bytes).unwrap();

        // Default values should be used
        assert_eq!(success_meta.server, "");
        assert_eq!(success_meta.connection_id, "");
    }

    #[test]
    fn should_create_meta_with_builder() {
        let meta = MetaBuilder::new()
            .server("Neo4j/4.2.0")
            .connection_id("bolt-42")
            .build();

        assert_eq!(meta.server, "Neo4j/4.2.0");
        assert_eq!(meta.connection_id, "bolt-42");
        assert_eq!(meta.done, true);
        assert_eq!(meta.has_more, false);
    }

    #[test]
    fn should_create_meta_with_custom_boolean_values() {
        let meta = MetaBuilder::new()
            .server("Neo4j/4.2.0")
            .connection_id("bolt-42")
            .done(false)
            .has_more(true)
            .build();

        assert_eq!(meta.server, "Neo4j/4.2.0");
        assert_eq!(meta.connection_id, "bolt-42");
        assert_eq!(meta.done, false);
        assert_eq!(meta.has_more, true);
    }

    #[test]
    fn should_create_meta_with_custom_fields() {
        let meta = MetaBuilder::new()
            .server("Neo4j/4.2.0")
            .field("qid".to_string())
            .field("db".to_string())
            .build();

        assert_eq!(meta.server, "Neo4j/4.2.0");
        assert!(meta.fields.contains(&("qid".to_string())));
        assert!(meta.fields.contains(&("db".to_string())));
    }

    #[test]
    fn should_create_meta_with_multiple_fields() {
        let fields = vec![
            "qid".to_string(),
            "db".to_string(),
            "query_time".to_string(),
        ];

        let meta = MetaBuilder::new().fields(fields).build();

        assert_eq!(meta.fields.len(), 3);
        assert!(meta.fields.contains(&("qid".to_string())));
        assert!(meta.fields.contains(&("db".to_string())));
        assert!(meta.fields.contains(&("query_time".to_string())));
    }
}
