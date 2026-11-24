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
import java.util.concurrent.Executor;
import javax.annotation.Nullable;

public class EventRunUtil {
  /**
   * Runs Event functions of all EventProfiles of a Flow for a single Event. See {@link
   * com.flower.anno.event.Concurrency} for the execution order.
   *
   * @param functionsByProfile Event functions, grouped by EventProfile
   * @return future that completes when all functions except NON_BLOCKING ones are done; fails only if a
   *     FINALIZER function fails
   */
  public static CompletableFuture<Void> runEvents(
      List<List<EventFunctionContext>> functionsByProfile,
      EventParametersProvider eventParametersProvider,
      EventType eventType,
      @Nullable Transition transition,
      @Nullable Throwable flowException,
      Executor scheduler) {
    if (functionsByProfile.stream().allMatch(List::isEmpty)) {
      return CompletableFuture.completedFuture(null);
    }

    List<EventFunctionContext> nonBlockingFunctions = new ArrayList<>();
    List<EventFunctionContext> finalizerFunctions = new ArrayList<>();
    List<List<EventFunctionContext>> blockingByProfile = new ArrayList<>();
    List<List<EventFunctionContext>> synchronizedByProfile = new ArrayList<>();
    int profilesWithSynchronized = 0;

    for (List<EventFunctionContext> profileFunctions : functionsByProfile) {
      List<EventFunctionContext> blockingFunctions = new ArrayList<>();
      List<EventFunctionContext> synchronizedFunctions = new ArrayList<>();
      for (EventFunctionContext function : profileFunctions) {
        switch (function.eventFunction.concurrency) {
          case NON_BLOCKING:
            nonBlockingFunctions.add(function);
            break;
          case BLOCKING:
            blockingFunctions.add(function);
            break;
          case SYNCHRONIZED:
            synchronizedFunctions.add(function);
            break;
          case FINALIZER:
            finalizerFunctions.add(function);
            break;
        }
      }
      if (!blockingFunctions.isEmpty() || !synchronizedFunctions.isEmpty()) {
        blockingByProfile.add(blockingFunctions);
        synchronizedByProfile.add(synchronizedFunctions);
        if (!synchronizedFunctions.isEmpty()) {
          profilesWithSynchronized++;
        }
      }
    }

    // 1) Fire and forget NON_BLOCKING functions of all EventProfiles.
    CompletableFuture<Void> unusedNonBlocking =
        runParallel(
            nonBlockingFunctions, eventParametersProvider, eventType, transition, flowException,
            scheduler);

    // 2) + 3) Per EventProfile: BLOCKING functions in parallel, then SYNCHRONIZED sequentially.
    // Different EventProfiles proceed independently (SYNCHRONIZED functions only change their own
    // EventProfile's state, so that's safe).
    //
    // Which thread SYNCHRONIZED functions are called on:
    // - more than one EventProfile has SYNCHRONIZED functions: each call is submitted to the
    //   scheduler. Otherwise a synchronous function of one EventProfile would occupy the current
    //   thread and hold back the other EventProfiles, i.e. they wouldn't run in parallel.
    // - at most one EventProfile has them: there's nothing to run in parallel with, so
    //   synchronizedExecutor is null and each function is called directly on the thread that
    //   completes the previous stage (no task submission / thread hop).
    // Either way, functions of a single EventProfile run sequentially, and the returned future
    // waits for all of them.
    @Nullable Executor synchronizedExecutor = profilesWithSynchronized > 1 ? scheduler : null;
    List<CompletableFuture<Void>> profileFutures = new ArrayList<>();
    for (int i = 0; i < blockingByProfile.size(); i++) {
      List<EventFunctionContext> synchronizedFunctions = synchronizedByProfile.get(i);
      profileFutures.add(
          FuturesTool.transformAsync(
              runParallel(
                  blockingByProfile.get(i),
                  eventParametersProvider,
                  eventType,
                  transition,
                  flowException,
                  scheduler),
              void_ ->
                  runSequential(
                      synchronizedFunctions,
                      eventParametersProvider,
                      eventType,
                      transition,
                      flowException,
                      true,
                      synchronizedExecutor),
              directExecutor()));
    }

    // 4) + 5) Once all EventProfiles are done, FINALIZER functions run sequentially; a failure
    // fails the returned future (and the Flow).
    return FuturesTool.transformAsync(
        FuturesTool.whenAllComplete(profileFutures),
        void_ ->
            runSequential(
                finalizerFunctions,
                eventParametersProvider,
                eventType,
                transition,
                flowException,
                false,
                null),
        directExecutor());
  }

  /**
   * Runs {@code functions} one after another; each starts once the previous one is done.
   *
   * @param invokeExecutor if not null, each function is called on this executor
   */
  static CompletableFuture<Void> runSequential(
      List<EventFunctionContext> functions,
      EventParametersProvider eventParametersProvider,
      EventType eventType,
      @Nullable Transition transition,
      @Nullable Throwable flowException,
      boolean hideExceptions,
      @Nullable Executor invokeExecutor) {
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
                      flowException,
                      invokeExecutor),
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

  /**
   * Starts all {@code functions} at once, each called on {@code executor}, so that synchronous
   * functions run concurrently. Parameters are read on the current thread. The returned future
   * completes when all functions are done, and never fails.
   */
  static CompletableFuture<Void> runParallel(
      List<EventFunctionContext> functions,
      EventParametersProvider eventParametersProvider,
      EventType eventType,
      @Nullable Transition transition,
      @Nullable Throwable flowException,
      Executor executor) {
    if (functions.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
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
                  flowException,
                  executor),
              Throwable.class,
              e ->  {
                // TODO: exception handling - log?
                return CompletableFuture.<Void>completedFuture(null);
              },
              directExecutor()));
    }

    return FuturesTool.whenAllComplete(futures);
  }

  static CompletableFuture<Void> invokeEventFunction(
      ObjectStateAccess eventProfileStateAccess,
      ObjectStateAccess flowStateAccess,
      FunctionCallContext functionCallContext,
      EventParametersProvider eventParametersProvider,
      EventType eventType,
      @Nullable Transition transition,
      @Nullable Throwable flowException,
      @Nullable Executor invokeExecutor) {
    try {
      EventFunctionCallState callState =
          new EventFunctionCallState(flowStateAccess, eventProfileStateAccess);
      return FunctionCallUtil.invokeEventFunction(
          callState,
          functionCallContext,
          eventParametersProvider,
          eventType,
          transition,
          flowException,
          invokeExecutor);
    } catch (Throwable t) {
      return CompletableFuture.failedFuture(t);
    }
  }
}
