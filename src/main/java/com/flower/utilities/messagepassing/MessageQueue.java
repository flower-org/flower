package com.flower.utilities.messagepassing;

import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nullable;
import java.util.Collection;

/**
 * Provides a basic queue structure to be used for message passing.
 * Gives the party that adds a message a CompletableFuture connected to message processing result.
 *
 * @param <M> Message
 * @param <R> Processing result
 */
public class MessageQueue<M, R> extends AbstractMessageQueue<Pair<M, CompletableFuture<R>>> implements MessageSink<M, R>, MessageSource<M, R> {
    protected CompletableFuture<R> innerAddWithoutNotify(M message) {
        CompletableFuture<R> future = new CompletableFuture<>();
        innerQueue.add(Pair.of(message, future));
        return future;
    }

    @Override
    public CompletableFuture<R> add(M message) {
        CompletableFuture<R> resultFuture = innerAddWithoutNotify(message);
        notifyMessageListeners(false);
        return resultFuture;
    }

    @Override
    public Collection<CompletableFuture<R>> addAll(Collection<? extends M> messages) {
        Collection<CompletableFuture<R>> resultFutures = messages.stream().map(
            this::innerAddWithoutNotify
        ).toList();
        notifyMessageListeners(false);
        return resultFutures;
    }

    /**
     * Returns a pair of message and CompletableFuture to the Flow.
     * We expect the Flow to set result value to the Future to send back notification about message processing.
     */
    @Override
    @Nullable public Pair<M, CompletableFuture<R>> poll() {
        return innerPoll();
    }

    @Override
    @Nullable public M peek() {
        Pair<M, CompletableFuture<R>> pair = innerPeek();
        return pair == null ? null : pair.getLeft();
    }
}
