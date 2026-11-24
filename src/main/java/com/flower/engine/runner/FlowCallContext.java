package com.flower.engine.runner;

import com.flower.anno.event.EventType;
import com.flower.conf.FlowId;
import com.flower.conf.StepInfoPrm;
import com.flower.conf.Transition;
import com.flower.engine.FlowImpl;
import com.flower.engine.runner.event.EventContext;
import com.flower.engine.runner.event.EventExceptionReporter;
import com.flower.engine.runner.event.EventFunctionContext;
import com.flower.engine.runner.event.EventFunctions;
import com.flower.engine.runner.event.EventParametersProvider;
import com.flower.engine.runner.event.EventProfileForFlowTypeCallContext;
import com.flower.engine.runner.event.EventRunUtil;
import com.flower.engine.runner.event.EventRunner;
import com.flower.engine.runner.step.InternalTransition;
import com.flower.engine.runner.step.StepCallContext;
import com.flower.engine.runner.state.StateAccessConfig;
import com.flower.eventbus.FlowEventBus;
import com.google.common.base.Preconditions;
import com.flower.utilities.FuturesTool;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nullable;

import org.apache.commons.lang3.tuple.Pair;
import java.util.concurrent.Executor;

public class FlowCallContext implements EventRunner {
  final Map<String, StepCallContext> stepCalls;
  final Map<Class<?>, EventProfileForFlowTypeCallContext> eventContexts;
  // final
  final String firstStepName;

  final String flowName;
  final Class<?> flowType;
  final ScheduledExecutorService scheduler;
  /** Whether any function of this Flow type takes an @EventBus parameter. */
  final boolean usesEventBus;
  final StateAccessConfig flowStateAccessConfig;
  final EventExceptionReporter eventExceptionReporter;
  /** Executor for stages that call user functions, see SchedulerAffineExecutor. */
  final Executor functionExecutor;

  public FlowCallContext(
      String flowName,
      Class<?> flowType,
      Map<String, StepCallContext> stepCalls,
      String firstStepName,
      Map<Class<?>, EventProfileForFlowTypeCallContext> eventContexts,
      StateAccessConfig flowStateAccessConfig,
      EventExceptionReporter eventExceptionReporter,
      Executor functionExecutor,
      ScheduledExecutorService scheduler) {
    this.flowName = flowName;
    this.functionExecutor = functionExecutor;
    this.eventExceptionReporter = eventExceptionReporter;
    this.flowStateAccessConfig = flowStateAccessConfig;
    this.flowType = flowType;
    this.stepCalls = stepCalls;
    this.firstStepName = firstStepName;
    this.eventContexts = eventContexts;
    this.scheduler = scheduler;
    this.usesEventBus = stepCalls.values().stream().anyMatch(StepCallContext::usesEventBus);
    if (!stepCalls.containsKey(firstStepName))
      throw new IllegalStateException(
          String.format(
              "Fatal: firstStepName is not contained in steps: flow[%s] step [%s]",
              flowName, firstStepName));
  }

  <T> FlowImpl<T> createFlow(FlowId id, T flowState)
      throws IllegalAccessException, InstantiationException, InvocationTargetException {
    flowStateAccessConfig.verifyInitializedOnCreation(
        flowState, String.format("Flow [%s]", flowName));

    Map<Class<?>, Object> eventProfileStates = new HashMap<>();
    for (EventProfileForFlowTypeCallContext eventContext : eventContexts.values()) {
      String eventProfileName = eventContext.getEventProfileName();
      Class<?> eventProfileContainerType = eventContext.getEventProfileContainerType();

      try {
        Constructor<?> constructor = eventProfileContainerType.getConstructor();
        constructor.setAccessible(true);
        eventProfileStates.put(eventProfileContainerType, constructor.newInstance());
      } catch (NoSuchMethodException nsme) {
        try {
          eventProfileStates.put(eventProfileContainerType, eventProfileContainerType.newInstance());
        } catch(IllegalAccessException iae) {
          //TODO: figure why this is happening and fix. Reproducible in StateUpdateOnErrorTest.partialStateUpdate5Test() / OnErrorEventProfile.
          throw new IllegalStateException("This error happens when EventProfile class doesn't have a default constructor, please add default constructor.", iae);
        }
      }
      eventContext
          .getEventProfileStateAccessConfig()
          .verifyInitializedOnCreation(
              Preconditions.checkNotNull(eventProfileStates.get(eventProfileContainerType)),
              String.format("EventProfile [%s]", eventProfileName));
    }

    return new FlowImpl<>(
        id,
        flowName,
        flowType,
        Optional.empty(),
        firstStepName,
        flowState,
        eventProfileStates,
        usesEventBus ? new FlowEventBus() : null);
  }

  @Override
  public CompletableFuture<Void> runEvents(
      EventType eventType,
      EventContext eventContext,
      EventParametersProvider eventParametersProvider,
      @Nullable Transition transition,
      @Nullable Throwable flowException) {
    List<List<EventFunctionContext>> eventFunctionsByProfile = new ArrayList<>();

    for (Map.Entry<Class<?>, EventProfileForFlowTypeCallContext> entry : eventContexts.entrySet()) {
      Class<?> profileNameType = entry.getKey();
      EventProfileForFlowTypeCallContext callContext = entry.getValue();
      Object eventProfileState =
          Preconditions.checkNotNull(eventContext.getEventProfileStates().get(profileNameType));

      EventFunctions eventFunctions = callContext.getFunctionsForEventType(eventType);
      if (eventFunctions != null)
        eventFunctionsByProfile.add(
            eventFunctions.getFunctions(
                eventContext.getFlowState(), eventProfileState, callContext.getEventProfileName()));
    }

    return EventRunUtil.runEvents(
        eventFunctionsByProfile,
        eventParametersProvider,
        eventType,
        transition,
        flowException,
        scheduler,
        eventExceptionReporter,
        functionExecutor);
  }

  <T> Pair<FlowImpl<T>, CompletableFuture<InternalTransition>> runFirstStep(FlowImpl<T> flow) {
    return Pair.of(
        flow,
        FuturesTool.transformAsync(
            FuturesTool.transformAsync(
                runEvents(EventType.BEFORE_FLOW, flow, flow, null, null),
                void_ -> {
                  // TODO: some duplication here?
                  flow.setCurrentStep(firstStepName);
                  StepInfoPrm stepInfo = getStep(firstStepName).getStepInfo();
                  flow.setStepInfo(stepInfo);

                  return runEvents(EventType.BEFORE_STEP, flow, flow, null, null);
                },
                //important to context switch here, to untie the calling thread, initiating the flow execution.
                scheduler),
            void_ -> runStep(flow, firstStepName),
            scheduler));
  }

  StepCallContext getStep(String stepName) {
    return Preconditions.checkNotNull(stepCalls.get(stepName));
  }

  CompletableFuture<InternalTransition> runStep(FlowImpl flow, String stepName) {
    try {
      StepCallContext stepCallContext = getStep(stepName);

      return FuturesTool.transformAsync(
          FuturesTool.transformAsync(
              runEvents(EventType.BEFORE_STEP_ITERATION, flow, flow, null, null),
              void_ -> stepCallContext.call(
                      flow.getId(),
                      flow.getState(),
                      this,
                      flow,
                      flow,
                      flow.getEventBus(),
                      functionExecutor),
              scheduler),
          t ->
              FuturesTool.transform(
                  runEvents(EventType.AFTER_STEP_ITERATION, flow, flow, null, null),
                  void_ -> t,
                  scheduler),
          scheduler);
    } catch (Throwable t) {
      return CompletableFuture.failedFuture(t);
    }
  }
}
