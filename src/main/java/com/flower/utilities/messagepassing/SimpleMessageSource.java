package com.flower.utilities.messagepassing;

import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

public interface SimpleMessageSource<M> {
    @Nullable M poll();

    @Nullable M peek();

    CompletableFuture<Void> getMessageListener();
}
