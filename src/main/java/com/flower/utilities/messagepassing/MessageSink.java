package com.flower.utilities.messagepassing;

import java.util.concurrent.CompletableFuture;

import java.util.Collection;

/**
 * This interface is introduced to bar Message Producer from using Consumer methods
 */
public interface MessageSink<M, R> {
    CompletableFuture<R> add(M message);
    Collection<CompletableFuture<R>> addAll(Collection<? extends M> messages);

    /**
     * Wake up all waiting consumers without sending a message
     */
    void notifyMessageListeners();
}
