We are designing and developing a message forwarding module (`uevtd`) in Rust. It reads messages from a Unix Domain Socket (UDS), persists them locally, and forwards them to configured channels. The functional requirements are as follows:

1. **Inbound**: Read messages from a configurable UDS path. Each message is typically a JSON string. At this stage, complex message parsing or processing is not required, and normal messages can be assumed to be complete and valid. However, connection and disconnection edge cases must be handled properly—for example, truncated messages must be discarded and logged.
2. **Storage**: Persist received messages locally with a configurable storage path and size limit. When the size limit is exceeded, older data must be evicted without corrupting individual message integrity. The storage layout and data structures must be carefully analyzed and designed for robustness and efficiency. Write traffic is sparse and low-frequency over long time horizons, with bursty spikes (up to 1,000 messages/s) and relatively small payloads (~0.5 KB per message). Reads are low-frequency and typically consist of time-ordered range queries served via a dedicated HTTP API.
3. **Outbound**: Forward messages to target channels based on configuration:
    - **HTTP**: Send messages via HTTP requests to a specified endpoint, with configurable URL, HTTP method, headers, and timeout.
    - **MQTT**: Publish messages via MQTT to a specified topic, with configurable broker address, port, topic, and connection parameters.
4. **Configuration**: Expose an HTTP API (over UDS) for external callers to retrieve and update module configurations, covering Inbound, Storage, and Outbound parameters. Configuration updates must be applied dynamically without disrupting in-flight message processing, and must be persisted to a specified file path.

Engineering Constraints:
1. **Minimalism, performance, and stability first**: Prefer modern, mainstream, battle-tested Rust crates and technical approaches while keeping dependencies minimal.
2. **Modularity**: Keep functional modules decoupled, auditable, and easy to maintain and extend.
3. **Core principles**: Correctness, efficiency (high performance), robustness, and consistency (coherency and simplicity).
4. **Maintainability**: Uphold sound architectural design and exceptionally high code quality (in the spirit of Jeff Dean and Donald Knuth), avoiding redundant or noisy comments.
5. **Logging and observability**: Provide comprehensive logging and monitoring capabilities to facilitate troubleshooting and performance analysis.

The functional requirements and engineering constraints above serve as guiding principles for designing and implementing the message forwarding module, though practical trade-offs and adjustments may still be needed during implementation. Please raise any questions or suggestions for discussion promptly, and start by completing the architectural design plan.

Dev machine: ssh dev (in container, the dev/uevtd folder is mounted as volume under home)
