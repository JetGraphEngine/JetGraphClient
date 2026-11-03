//! Bounds for queries the agent is allowed to send.

const MAX_CYPHER_ROWS: u64 = 100;
const WRITE_WORDS: &[&str] = &[
    "create", "merge", "delete", "detach", "set", "remove", "drop", "foreach", "load",
];

/// Read-only Cypher with a row cap. A missing LIMIT is added at [`MAX_CYPHER_ROWS`].
pub fn prepare_cypher(query: &str) -> Result<String, String> {
    let query = one_statement(query)?;
    reject_writes(&query)?;
    reject_non_graph_calls(&query)?;
    let limited = cap_limit(&query)?;
    Ok(limited)
}

/// A signal query binds `$node`, returns `value`, and does not write.
pub fn validate_signal_query(query: &str) -> Result<(), String> {
    let query = one_statement(query)?;
    if !query.contains("$node") {
        return Err("a signal query must bind $node".into());
    }
    if !has_value_column(&query) {
        return Err("a signal query must return one column named value".into());
    }
    reject_writes(&query)?;
    reject_non_graph_calls(&query)?;
    if query.len() > 4_000 {
        return Err("signal query is longer than 4000 characters".into());
    }
    Ok(())
}

pub fn require_name(name: &str) -> Result<(), String> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && name.chars().next().is_some_and(|c| c.is_ascii_alphabetic());
    if ok {
        Ok(())
    } else {
        Err(
            "name must start with a letter and contain only letters, digits, and underscores"
                .into(),
        )
    }
}

pub fn clamp_page(limit: Option<u32>, default: u32, max: u32) -> Result<u32, String> {
    match limit {
        None => Ok(default),
        Some(0) => Err(format!("limit must be from 1 to {max}")),
        Some(value) if value > max => Err(format!("limit {value} exceeds the maximum of {max}")),
        Some(value) => Ok(value),
    }
}

fn one_statement(query: &str) -> Result<String, String> {
    let query = query.trim();
    let query = query.strip_suffix(';').unwrap_or(query).trim();
    if query.is_empty() {
        return Err("query is empty".into());
    }
    if query.contains(';') {
        return Err("only one statement is accepted".into());
    }
    if query.len() > 8_000 {
        return Err("query is longer than 8000 characters".into());
    }
    Ok(query.to_string())
}

fn reject_writes(query: &str) -> Result<(), String> {
    let lower = query.to_ascii_lowercase();
    for word in WRITE_WORDS {
        if contains_word(&lower, word) {
            return Err(format!(
                "query contains '{word}', which this server does not run"
            ));
        }
    }
    Ok(())
}

fn reject_non_graph_calls(query: &str) -> Result<(), String> {
    let lower = query.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut index = 0;
    while let Some(found) = lower[index..].find("call") {
        let at = index + found;
        let before_ok = at == 0 || !is_ident(bytes[at - 1]);
        let after = at + 4;
        let after_ok = after >= bytes.len() || !is_ident(bytes[after]);
        if before_ok && after_ok {
            let tail = lower[after..].trim_start();
            if !tail.starts_with("graph.") {
                return Err("only CALL graph.* procedures are accepted".into());
            }
        }
        index = after;
        if index >= lower.len() {
            break;
        }
    }
    Ok(())
}

fn cap_limit(query: &str) -> Result<String, String> {
    let lower = query.to_ascii_lowercase();
    let mut search_from = 0;
    let mut found = false;
    while let Some(at) = lower[search_from..].find("limit") {
        let abs = search_from + at;
        let before_ok = abs == 0 || !is_ident(lower.as_bytes()[abs - 1]);
        let after = abs + 5;
        let after_ok = after >= lower.len() || !is_ident(lower.as_bytes()[after]);
        if before_ok && after_ok {
            found = true;
            let rest = query[after..].trim_start();
            let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
            if digits.is_empty() {
                return Err("LIMIT must be a number".into());
            }
            let rows: u64 = digits
                .parse()
                .map_err(|_| "LIMIT is not a number".to_string())?;
            if rows == 0 || rows > MAX_CYPHER_ROWS {
                return Err(format!("LIMIT must be from 1 to {MAX_CYPHER_ROWS}"));
            }
        }
        search_from = after;
        if search_from >= lower.len() {
            break;
        }
    }
    if found {
        Ok(query.to_string())
    } else {
        Ok(format!("{query} LIMIT {MAX_CYPHER_ROWS}"))
    }
}

fn has_value_column(query: &str) -> bool {
    let lower = query.to_ascii_lowercase();
    contains_word(&lower, "value")
}

fn contains_word(haystack: &str, word: &str) -> bool {
    let bytes = haystack.as_bytes();
    let mut from = 0;
    while let Some(at) = haystack[from..].find(word) {
        let abs = from + at;
        let before_ok = abs == 0 || !is_ident(bytes[abs - 1]);
        let after = abs + word.len();
        let after_ok = after >= bytes.len() || !is_ident(bytes[after]);
        if before_ok && after_ok {
            return true;
        }
        from = abs + word.len();
        if from >= haystack.len() {
            break;
        }
    }
    false
}

fn is_ident(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adds_a_limit_when_the_query_has_none() {
        let query = prepare_cypher("MATCH (n:card) RETURN n.external_id").unwrap();
        assert!(query.ends_with("LIMIT 100"));
    }

    #[test]
    fn keeps_a_small_limit() {
        let query = prepare_cypher("MATCH (n:card) RETURN n.external_id LIMIT 10").unwrap();
        assert!(query.ends_with("LIMIT 10"));
    }

    #[test]
    fn rejects_a_large_limit_and_writes() {
        assert!(prepare_cypher("MATCH (n) RETURN n LIMIT 1000").is_err());
        assert!(prepare_cypher("CREATE (n:card {external_id:'a'})").is_err());
        assert!(prepare_cypher("MATCH (n) SET n.x = 1 RETURN n LIMIT 1").is_err());
        assert!(prepare_cypher("CALL db.labels() YIELD label RETURN label LIMIT 5").is_err());
    }

    #[test]
    fn allows_a_graph_procedure() {
        let query = prepare_cypher(
            "CALL graph.histogram($node, 'BOUGHT', 1, null) YIELD total_events RETURN total_events LIMIT 1",
        )
        .unwrap();
        assert!(query.contains("graph.histogram"));
    }

    #[test]
    fn signal_query_must_bind_node_and_value() {
        assert!(validate_signal_query(
            "CALL graph.histogram($node, 'BOUGHT', 1, null) YIELD total_events RETURN total_events AS value"
        )
        .is_ok());
        assert!(validate_signal_query("MATCH (n) RETURN n.external_id AS value").is_err());
        assert!(validate_signal_query("MATCH (n) WHERE id(n) = $node DELETE n").is_err());
    }

    #[test]
    fn names_are_identifiers() {
        assert!(require_name("buys_last_1h").is_ok());
        assert!(require_name("2buys").is_err());
        assert!(require_name("has space").is_err());
    }
}
