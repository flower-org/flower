package com.flower.conf;

import java.util.concurrent.CompletableFuture;

public interface FlowFuture<T> {
  FlowId getFlowId();

  CompletableFuture<T> getFuture();
}
