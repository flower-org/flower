package com.flower.validations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.State;
import com.flower.anno.functions.StepFunction;
import com.flower.anno.functions.TransitFunction;
import com.flower.anno.params.common.Out;
import com.flower.anno.params.transit.Terminal;
import com.flower.conf.OutPrm;
import com.flower.conf.Transition;
import com.flower.engine.Flower;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

/** Asynchronous Flower functions must return CompletableFuture; other future types are rejected. */
public class AsyncReturnTypeTest {
  static void assertRejected(Class<?> flowType, String returnType) {
    Flower flower = new Flower();
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> {
              flower.registerFlow(flowType);
              flower.initialize();
            });
    assertTrue(
        e.getMessage().contains("Function return type [" + returnType + "] is not supported"),
        e.getMessage());
  }

  @Test
  public void stepReturningListenableFuture_rejected() {
    assertRejected(ART_ListenableFutureStep.class, ListenableFuture.class.getName());
  }

  @Test
  public void transitReturningFuture_rejected() {
    assertRejected(ART_FutureTransit.class, Future.class.getName());
  }

  @Test
  public void stepReturningCompletionStage_rejected() {
    assertRejected(ART_CompletionStageStep.class, CompletionStage.class.getName());
  }

  @Test
  public void completableFutureFunctions_accepted() throws Exception {
    Flower flower = new Flower();
    flower.registerFlow(ART_CompletableFutureFlow.class);
    flower.initialize();

    ART_CompletableFutureFlow flow =
        flower
            .getFlowExec(ART_CompletableFutureFlow.class)
            .runFlow(new ART_CompletableFutureFlow())
            .getFuture()
            .get();
    assertEquals(42, flow.result);
    flower.shutdownScheduler();
  }
}

@FlowType(firstStep = "step")
class ART_ListenableFutureStep {
  @StepFunction(transit = "transit")
  static ListenableFuture<Integer> step() {
    return Futures.immediateFuture(1);
  }

  @TransitFunction
  static Transition transit(@Terminal Transition end) {
    return end;
  }
}

@FlowType(firstStep = "step")
class ART_FutureTransit {
  @StepFunction(transit = "transit")
  static void step() {}

  @TransitFunction
  static Future<Transition> transit(@Terminal Transition end) {
    return CompletableFuture.completedFuture(end);
  }
}

@FlowType(firstStep = "step")
class ART_CompletionStageStep {
  @StepFunction(transit = "transit")
  static CompletionStage<Integer> step() {
    return CompletableFuture.completedFuture(1);
  }

  @TransitFunction
  static Transition transit(@Terminal Transition end) {
    return end;
  }
}

@FlowType(firstStep = "step")
class ART_CompletableFutureFlow {
  @State Integer result;

  @StepFunction(transit = "transit")
  static CompletableFuture<Void> step(@Out OutPrm<Integer> result) {
    result.setOutFuture(CompletableFuture.supplyAsync(() -> 42));
    return CompletableFuture.completedFuture(null);
  }

  @TransitFunction
  static CompletableFuture<Transition> transit(@Terminal Transition end) {
    return CompletableFuture.completedFuture(end);
  }
}
