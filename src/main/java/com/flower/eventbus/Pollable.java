package com.flower.eventbus;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Read side of an update stream: long-polls for numbered updates. Consumers that only read (e.g. a Flow
 * re-registering polls on its {@link FlowEventBus}) depend on this rather than on the producer side.
 */
public interface Pollable<V> {
    record Update<V>(long updateNo, V value) {}

    /**
     * Returns the updates with {@code updateNo > lastUpdate}, or, if there are none yet, a future that
     * completes with the next update(s). Pass {@code 0} to get everything available. An empty list means
     * the stream is closed.
     */
    CompletableFuture<List<Update<V>>> poll(long lastUpdate);
}
