JetGraph MCP: how to analyze JetGraph data

JetGraph is a real-time graph that keeps aggregated behavior, not an event log. These instructions are schema-independent: never assume a node type, edge type, or property name. Read them from get_schema, then follow the rules below. Everything here applies to any subject area.

1. START HERE: DISCOVER THE SCHEMA (get_schema)
- Always call get_schema first, and again whenever a name is rejected. Map the words in the user's question to types through each type's description and business_meaning. Ask the user only when two types match equally well.
- Each node_type has: name, external_id kind ('string' or 'numeric'), description, business_meaning, and registered properties ([{name, value_type}], value_type is int | float | string | bool | timestamp). In Cypher every node also exposes n.external_id and id(n); neither is listed in properties.
- Each edge_type has: name, from -> to node types, description, business_meaning, symmetric, minimal_payload, state_ttl_secs, activity_tick_secs, activity_horizon_secs, bool_property, tracked_property, bin_boundaries, node_histogram, and ingest properties.
- get_schema also returns feature_schemas (32-slot node vector definitions) and transaction_profiles.
- Before using a tool, check the schema flag it depends on: node_histogram.enabled_for_src for get_histogram, feature_schemas[].enabled for get_feature_vector, minimal_payload for any activity or bin reasoning, state_ttl_secs for any "never seen" claim.

2. THE JETGRAPH DATA MODEL: ONE COMPACT RECORD PER PAIR
JetGraph does NOT store one edge per event. Every (src, dst) pair on an edge type is one record updated in place. There are three record shapes, chosen per edge type:
- Compact (tracked_property is set): tx_count, approx_sum, last_seen, bool_flag (only if bool_property is set), 8 value bins, and a 21-tick activity bitmap.
- Slim (no tracked_property): tx_count, last_seen, bool_flag, and the activity bitmap. No value bins, and approx_sum is 0. Use it for counting and recency, never for amounts.
- Static (minimal_payload: true; typically membership or similarity edges): only a value (approx_sum, a score or confidence) and last_seen. No bitmap, no bins, and no real event count (tx_count reads as 1). activity_horizon_secs is 0. Do not ask a static edge for activity windows.
- Symmetric edges (symmetric: true) are undirected: one canonical record per pair and neighbors come back from both sides. Both endpoints have the same node type, so direction does not matter.
Cypher fields on a relationship variable r: r.tx_count (lifetime events), r.approx_sum (alias r.amount; accumulated tracked value), r.last_seen (epoch seconds), r.bool_flag, r.bins (list of 8 counts). Edge ingest properties are NOT stored; r.weight or r.<bool_property> evaluates to NULL. Use the fields above.
Counting rules, because 1 record = 1 pair:
- count(r) counts distinct counterparties.
- sum(r.tx_count) counts total events.
- sum(r.approx_sum) sums the tracked value.
- Quote these three separately; they answer different questions ("how many partners", "how many events", "how much value").
Two different bin sets exist. r.bins is per pair (this src with this dst). get_histogram bin_counts is per source node across all its counterparties. Both use the same 8 buckets from bin_boundaries: bin 0 is < b[0], bin i is b[i-1] <= x < b[i], bin 7 is >= b[6].

3. PRECISION AND LIMITS: WHAT THE NUMBERS CAN AND CANNOT SAY
- Activity ticks saturate: each tick of the bitmap is a 3-bit counter that stops at 7. A tick with 7 may have had many more events. Treat every activity_counts value as a lower bound, and say so when a tick may be saturated (a count near 7 x number of ticks). Node histograms saturate too: each bin in each hourly or daily slot is an 8-bit counter capped at 255, which matters most for very busy nodes (destination-side histograms are the usual case). If one bin of a window looks flat at a multiple of 255, treat total_events and that bin as lower bounds. Use tx_count for exact per-pair volume.
- Lifetime counters are bounded: tx_count is a 32-bit counter, and approx_sum is a 32-bit float, so it loses precision for very large totals and many tiny additions. Report approx_sum as approximate. JetGraph is not the system of record for exact sums.
- Expiry: when state_ttl_secs > 0, a pair idle longer than that is deleted. When state_ttl_secs is 0, pairs are kept. So "found: false" means "no live record", which is "never seen" only when state_ttl_secs is 0 or the question is about a period shorter than the TTL. Otherwise say "not seen within the retention window".
- Horizon: the activity bitmap covers 21 x activity_tick_secs. Longer windows are capped silently. Windows are rounded up to whole ticks, so the finest resolution is one tick.
- Node histogram windows: window_hours <= hourly_slots, window_days <= daily_slots (hard limits of 1440 hours and 365 days). A longer window is capped at what is stored; compare window_covered_secs with what you asked for.
- get_neighbor_count returns approximate: true when the count is estimated. Say "about N" in that case.
- Nothing about an edge is historical beyond these aggregates. You cannot list individual past events, and you cannot recover the order of events inside one tick.

4. TIME HANDLING
- Timestamps named last_seen, last_seen_secs, and server_time_secs are epoch SECONDS. created_at_us (get_neighbors) is epoch MICROSECONDS. Never compare the two without converting.
- You cannot judge recency without the current time. get_edge_state, get_last_neighbor, and get_histogram return server_time_secs; subtract to get age (age = server_time_secs - last_seen_secs). In Cypher use a parameter such as $cutoff computed from server_time_secs; there is no now() function.
- get_edge_state aligns the activity bitmap to now, so activity_counts means "events in the last N seconds, as of server_time_secs". Raw Cypher fields (r.activity_bitmap_lo / activity_bitmap_hi) are anchored at last_seen instead; do not decode them yourself.
- Report times in UTC and include the window you used ("last 24 h as of <time>").

5. TOOL CHOICE AND DIRECTION RULES
- Prefer point tools for point questions: get_neighbors (ids and created_at_us only; no payload, not ordered by recency), get_neighbor_count, get_last_neighbor, get_edge_state, get_histogram, get_feature_vector, find_similar. Use cypher_read when you need pair payloads for many neighbors, multi-hop paths, filters, or rankings.
- Direction: get_neighbors defaults to direction 'out'; pass 'in' when starting from an edge type's 'to' node. get_last_neighbor looks at outgoing edges only; for inbound recency use an anchored cypher_read with ORDER BY r.last_seen DESC LIMIT 1. get_histogram is meaningful only for the source ('from') node of the edge type; on a 'to'-only node it returns 0, which does not mean "no activity".
- Always use the same edge_type and the same window on every node you compare.

6. ANALYSIS PLAYBOOK (answer a question by combining these)
Profile an entity: get_schema, then for each edge type touching its node type: get_neighbor_count, get_last_neighbor, get_histogram (source side), and get_feature_vector when enabled. Summarize breadth (counterparties), volume (events), value (histogram bins or approx_sum), and recency.
Is this relationship new? get_edge_state. found: false is novelty only if the TTL rule in section 3 allows it. If found, compare tx_count with activity_counts: a small tx_count with all activity in the newest ticks is a recent relationship; a large tx_count with activity spread over many ticks is established.
Burst or acceleration: get_histogram for a short window (for example window_hours: 24) and a longer baseline (window_days within daily_slots) on the same node and edge type; compare rates per hour, not raw totals. A short-window rate far above the baseline rate is acceleration; far below is cooling.
Concentrated or spread: compare the node's get_histogram total_events with one pair's get_edge_state activity_counts over a similar window. Similar numbers mean the activity is concentrated on that counterparty; a large node total with small pair counts means it is spread across many.
Regularity vs bursts: when a node feature vector has both activity_tick_count (distinct active ticks, regularity) and event_count or histogram_count (events, volume), compare them. High events in one tick is a burst; the same events over many ticks is steady.
Value pattern: use bin_counts (node) or r.bins (pair) to describe the distribution. Quote bin ranges from bin_boundaries. Do not compute averages from bins and do not read total_approx_sum from graph.histogram as money (it equals the event count).
Rank or find the most/least: single-block Top-K cypher_read (section 7). Say that the result is the top candidates when you used the fallback.
Lookalikes: find_similar with edge_weights on the edge types that define behavior; the score is weighted shared-neighbor overlap. Use required_edge_types to force overlap, and read shared_neighbors with the score. It does not write SIMILAR_TO edges.
Paths and fan-out: cypher_read with anchored MATCH on a label plus external_id and a literal LIMIT. Use inline edge filters ([r:EDGE WHERE r.last_seen > $cutoff AND r.tx_count >= 2]) to prune by recency or volume.
Fraud or review cases: CALL graph.fraudContext($node) YIELD flagged, cases, case_count, max_risk_score. A node with flagged: false is not cleared; it only has no recorded case.
Always follow a surprising number with a second, independent check (a different tool, window, or edge type) before reporting it.

7. CYPHER RULES (cypher_read)
- Read-only. Supported clauses: MATCH, OPTIONAL MATCH, WHERE, WITH, UNWIND, RETURN, ORDER BY, literal LIMIT (1..=100; a parameterized LIMIT $k is rejected; a missing LIMIT becomes 100), inline edge filters, degree(n, 'EDGE', 'out'|'in'|'both'), and CALL graph.*.
- Anchor every point or path query on a label and an external_id filter, for example MATCH (n:<NodeType> {external_id: $id})-[r:<EDGE_TYPE>]->(m) so it does not scan the whole graph. Pass string values as parameters ($val); literals containing words such as 'set' or 'load' can be blocked by the read-only guard.
- Allowed procedures (all return one row unless noted): graph.edgeState(src, dst, 'EDGE'[, window tick counts, not seconds]) yields tx_count, approx_sum, last_seen, bool_flag, activity_counts; graph.lastNeighbor(node, 'EDGE') yields found, neighbor_node_id, last_seen_secs; graph.neighborCount(node, 'EDGE') yields count; graph.histogram(node, 'EDGE', hours_or_null, days_or_null) yields buckets, counts, total_events, peak_bin; graph.segmentContext(node[, 'EDGE']) yields per-node totals; graph.fraudContext(node) yields flagged, cases, case_count, max_risk_score; graph.findSimilar(...) yields similar nodes.
- Top-K: NEVER put an unanchored aggregation before WITH (MATCH ... WITH n, sum(...) AS s RETURN ...); it materializes every group and fails with MemoryLimitExceeded. Keep RETURN <key>, sum(...) AS s ORDER BY s DESC LIMIT 10 in a single block so the planner keeps only k rows.
- Ranking fallback: if a multi-hop or filtered global ranking exceeds the time budget, narrow candidates with 1-hop Top-K scans (LIMIT 100, page by page), then rank those candidates with an anchored WHERE n.external_id IN $ids query. State that you ranked the top candidates, not the whole graph.
- Output may be truncated (truncated: true in the result). Say so and narrow the query instead of presenting a partial list as complete.

8. STORED 32-SLOT FEATURE VECTORS (get_feature_vector)
- Call it only when get_schema.feature_schemas has enabled: true for that node_type (otherwise it errors).
- Each slot has: index, name, raw, effective (after transform, weight, and fallback), support (how much evidence is behind it), and used_fallback. Prefer effective for comparing nodes; prefer raw for reporting the measured value. A slot with used_fallback: true or low support is a default, not an observation; do not treat it as a signal.
- Vector level fields: confidence (overall confidence in the vector; low means many slots lack evidence), age_secs (seconds since the vector was last computed; check it before trusting time-sensitive slots), quality_flags (a bit mask; non-zero means the engine marked a data-quality condition, so mention it and do not over-interpret), schema_version.
- Combine with section 6: regularity (activity_tick_count) versus volume (event_count) separates bursts from steady behavior.

9. SIGNALS AND SEGMENTS (list_signals, list_segments, create_signal, create_segment, list_segment_members)
- Call list_signals and list_segments first and reuse names; create_signal and create_segment REPLACE a definition with the same name. Test the measurement on a few nodes with the matching read tool before storing it.
- create_signal stores a read-only Cypher query that binds $node (internal node id) and returns one column named value:
  * Node property ('number'): MATCH (n) WHERE id(n) = $node RETURN n.prop_name AS value
  * Histogram window ('number', time_sensitive: true): CALL graph.histogram($node, 'EDGE', hours_or_null, days_or_null) YIELD total_events RETURN total_events AS value
  * Neighbor count ('number'): CALL graph.neighborCount($node, 'EDGE') YIELD count RETURN count AS value
  * Outbound recency ('epoch_secs', time_sensitive: true): CALL graph.lastNeighbor($node, 'EDGE') YIELD last_seen_secs RETURN last_seen_secs AS value
  Pair-level get_edge_state needs a destination node and cannot become a signal.
- Threshold units for epoch_secs: the evaluator converts the timestamp to elapsed DAYS ((now_secs - value) / 86400; null -> infinity) before comparing; op: 'gt', threshold: 30 means inactive for more than 30 days.
- create_segment combines signals with logic 'all', 'any', or 'weighted' (condition weight, min_confidence default 0.5; reason supports {value}, {threshold}, {delta}, {pct}). create_signal and create_segment change evaluator configuration only, not the graph. list_segment_members returns only nodes the evaluator has already processed, so an empty list right after creation is normal.

10. HOW TO REPORT
- State the definitions you used: edge types, properties or fields, windows (with server_time_secs), thresholds, and scores.
- Mark each figure as exact, approximate, or lower bound (sections 2 and 3), and name the reason.
- Separate observation (what the graph shows) from interpretation (what it may mean). Do not claim causation, intent, or identity from graph structure alone.
- A missing record, zero count, or empty page is "no live record in JetGraph", not proof of absence in the real world.
- When data is insufficient (disabled histogram, disabled feature vector, expired TTL, truncated result), say what is missing and which check would settle it.
