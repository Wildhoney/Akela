use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The wire format published on the Redis channel. Every instance receives
/// every envelope; `Send` fans out to matching local clients, whereas the tag
/// mutations apply only on the instance that owns the referenced client.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Envelope {
    Send {
        data: serde_json::Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tags: Option<BTreeSet<String>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sender: Option<Uuid>,
    },
    TagAdd {
        client: Uuid,
        tag: String,
    },
    TagRemove {
        client: Uuid,
        tag: String,
    },
}

/// Decides whether a client receives an event. A public event (`None` or an
/// empty set) reaches every client; otherwise the client must hold every
/// required tag, though it may hold any number of extras.
pub(crate) fn matches(required: Option<&BTreeSet<String>>, held: &BTreeSet<String>) -> bool {
    required.is_none_or(|tags| tags.is_subset(held))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tags(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn public_events_reach_every_client() {
        assert!(matches(None, &tags(&[])));
        assert!(matches(None, &tags(&["vip"])));
        assert!(matches(Some(&tags(&[])), &tags(&[])));
        assert!(matches(Some(&tags(&[])), &tags(&["vip"])));
    }

    #[test]
    fn requires_every_supplied_tag() {
        assert!(matches(Some(&tags(&["vip"])), &tags(&["vip"])));
        assert!(matches(
            Some(&tags(&["vip", "beta"])),
            &tags(&["vip", "beta"])
        ));
        assert!(!matches(Some(&tags(&["vip", "beta"])), &tags(&["vip"])));
    }

    #[test]
    fn clients_may_hold_extra_tags() {
        assert!(matches(
            Some(&tags(&["vip"])),
            &tags(&["vip", "beta", "gold"])
        ));
    }

    #[test]
    fn mismatched_tags_do_not_match() {
        assert!(!matches(Some(&tags(&["vip"])), &tags(&["beta"])));
        assert!(!matches(Some(&tags(&["vip"])), &tags(&[])));
    }
}
