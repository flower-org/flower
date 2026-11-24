package com.flower.engine.runner.event;

import static com.flower.utilities.FuturesTool.directExecutor;

import com.flower.anno.event.EventType;
import com.flower.conf.Transition;
import com.flower.engine.runner.callfunc.EventFunctionCallState;
import com.flower.engine.runner.callfunc.FunctionCallContext;
import com.flower.engine.runner.callfunc.FunctionCallUtil;
import com.flower.engine.runner.state.ObjectStateAccess;
import com.flower.utilities.FuturesTool;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

public class EventRunUtil {
  @SuppressWarnings("CheckReturnValue")
  public static CompletableFuture<Void> runEvents(
      List<EventFunctionContext> functions,
      EventParametersProvider eventParametersProvider,
      EventType eventType,
      @Nullable Transition transition,
      @Nullable Throwable flowException) {
    if (functions.isEmpty()) return CompletableFuture.completedFuture(null);

    List<EventFunctionContext> parallelFunctions = new ArrayList<>();
    List<EventFunctionContext> blockingFunctions = new ArrayList<>();
    List<EventFunctionContext> synchronizedFunctions = new ArrayList<>();
    List<EventFunctionContext> synchronizedBreakingFunctions = new ArrayList<>();

    for (EventFunctionContext function : functions) {
      switch (function.eventFunction.concurrency) {
        case PARALLEL:
          parallelFunctions.add(function);
          break;
        case BLOCKING:
          blockingFunctions.add(function);
          break;
        case SYNCHRONIZED:
          synchronizedFunctions.add(function);
          break;
        case FINALIZER:
          synchronizedBreakingFunctions.add(function);
          break;
      }
    }

    // TODO: only FINALIZER functions are awaited by the returned future; SYNCHRONIZED, BLOCKING and
    //  PARALLEL groups are started once FINALIZER functions complete, but their futures are not
    //  awaited (this preserves pre-CompletableFuture-migration behaviour).
    return FuturesTool.transform(
        runSequential(
            synchronizedBreakingFunctions,
            eventParametersProvider,
            eventType,
            transition,
            flowException,
            false),
        void_ -> {
          CompletableFuture<Void> unusedSynchronized =
              runSequential(
                  synchronizedFunctions,
                  eventParametersProvider,
                  eventType,
                  transition,
                  flowException,
                  true);
          CompletableFuture<Void> unusedBlocking =
              runParallel(
                  blockingFunctions, eventParametersProvider, eventType, transition, flowException);
          CompletableFuture<Void> unusedParallel =
              runParallel(
                  parallelFunctions, eventParametersProvider, eventType, transition, flowException);
          return null;
        },
        directExecutor());
  }

  static CompletableFuture<Void> runSequential(
      List<EventFunctionContext> functions,
      EventParametersProvider eventParametersProvider,
      EventType eventType,
      @Nullable Transition transition,
      @Nullable Throwable flowException,
      boolean hideExceptions) {
    CompletableFuture<Void> currentFuture = CompletableFuture.completedFuture(null);
    for (EventFunctionContext function : functions) {
      currentFuture =
          FuturesTool.transformAsync(
              currentFuture,
              void_ ->
                  invokeEventFunction(
                      function.eventProfileStateAccess,
                      function.flowStateAccess,
                      function.eventFunction.functionCallContext,
                      eventParametersProvider,
                      eventType,
                      transition,
                      flowException),
              directExecutor());

      if (hideExceptions) {
        currentFuture =
            FuturesTool.catchingAsync(
                currentFuture,
                Throwable.class,
                e -> CompletableFuture.completedFuture(null), // TODO: exception handling - log?
                directExecutor());
      }
    }
    return currentFuture;
  }

  static CompletableFuture<Void> runParallel(
      List<EventFunctionContext> functions,
      EventParametersProvider eventParametersProvider,
      EventType eventType,
      @Nullable Transition transition,
      @Nullable Throwable flowException) {
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (EventFunctionContext function : functions) {
      futures.add(
          FuturesTool.catchingAsync(
              invokeEventFunction(
                  function.eventProfileStateAccess,
                  function.flowStateAccess,
                  function.eventFunction.functionCallContext,
                  eventParametersProvider,
                  eventType,
                  transition,
                  flowException),
              Throwable.class,
              e ->  {
                // TODO: exception handling - log?
                return CompletableFuture.<Void>completedFuture(null);
              },
              directExecutor()));
    }

    return FuturesTool.whenAllCompleteCall(futures, () -> null, directExecutor());
  }

  static CompletableFuture<Void> invokeEventFunction(
      ObjectStateAccess eventProfileStateAccess,
      ObjectStateAccess flowStateAccess,
      FunctionCallContext functionCallContext,
      EventParametersProvider eventParametersProvider,
      EventType eventType,
      @Nullable Transition transition,
      @Nullable Throwable flowException) {
    try {
      EventFunctionCallState callState =
          new EventFunctionCallState(flowStateAccess, eventProfileStateAccess);
      return FunctionCallUtil.invokeEventFunction(
          callState,
          functionCallContext,
          eventParametersProvider,
          eventType,
          transition,
          flowException);
    } catch (Throwable t) {
      return CompletableFuture.failedFuture(t);
    }
  }
}
