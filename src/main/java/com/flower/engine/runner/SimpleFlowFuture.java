package com.flower.engine.runner;

import com.flower.conf.FlowFuture;
import com.flower.conf.FlowId;
import java.util.concurrent.CompletableFuture;

public class SimpleFlowFuture<T> implements FlowFuture<T> {
  final FlowId flowId;
  final CompletableFuture<T> future;

  public SimpleFlowFuture(FlowId flowId, CompletableFuture<T> future) {
    this.flowId = flowId;
    this.future = future;
  }

  @Override
  public FlowId getFlowId() {
    return flowId;
  }

  @Override
  public CompletableFuture<T> getFuture() {
    return future;
  }
}
