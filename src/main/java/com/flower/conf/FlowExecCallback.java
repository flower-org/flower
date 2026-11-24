package com.flower.conf;

import java.util.concurrent.CompletableFuture;

public interface FlowExecCallback {
  void flowStarted(FlowId flowId, CompletableFuture flow);

  void flowFinished(FlowId flowId);
}
