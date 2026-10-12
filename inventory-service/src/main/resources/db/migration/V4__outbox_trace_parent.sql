-- Spec 7.3: the W3C trace context of the request that wrote the row, so the relay can continue that trace.
-- Format: 00-<32 hex trace id>-<16 hex span id>-<2 hex flags> = 55 characters. NULL when nothing was traced.
ALTER TABLE outbox_event ADD COLUMN trace_parent VARCHAR(55);
