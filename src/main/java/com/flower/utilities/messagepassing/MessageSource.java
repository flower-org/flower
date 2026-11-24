package com.flower.utilities.messagepassing;

import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nullable;

public interface MessageSource<M, R> {
    /**
     * Returns a pair of message and CompletableFuture to the Flow.
     * We expect the Flow to set result value to the Future to send back notification about message processing.
     */
    @Nullable Pair<M, CompletableFuture<R>> poll();

    @Nullable M peek();

    CompletableFuture<Void> getMessageListener();
}
