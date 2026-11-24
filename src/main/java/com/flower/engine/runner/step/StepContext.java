package com.flower.engine.runner.step;

import static com.flower.utilities.FuturesTool.directExecutor;

import com.flower.anno.event.EventType;
import com.flower.conf.FlowId;
import com.flower.conf.StepInfoPrm;
import com.flower.engine.function.ParameterType;
import com.flower.engine.runner.callfunc.FunctionCallContext;
import com.flower.engine.runner.callfunc.FunctionCallUtil;
import com.flower.engine.runner.callfunc.StepFunctionCallState;
import com.flower.engine.runner.callfunc.TransitFunctionCallState;
import com.flower.engine.runner.callfunc.TransitFunctionExceptionCallState;
import com.flower.engine.runner.event.EventContext;
import com.flower.engine.runner.event.EventParametersProvider;
import com.flower.engine.runner.event.EventRunner;
import com.flower.engine.runner.event.StepInfo;
import com.flower.engine.runner.state.ObjectStateAccess;
import com.flower.engine.runner.state.StateAccess;
import com.flower.engine.runner.state.StateAccessConfig;
import com.flower.eventbus.FlowEventBus;
import com.flower.utilities.FuturesTool;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import javax.annotation.Nullable;

public class StepContext implements StepCallContext {
  final FunctionCallContext stepFunctionCall;
  final FunctionCallContext transitFunctionCall;
  final List<InternalTransition> transitions;
  final List<Pair<String, String>> flowFactories;
  final List<Pair<String, String>> flowRepos;

  @Nullable final String stepReturnValueToFlowParameterName;
  final StateAccessConfig flowStateAccessConfig;
  final boolean isTransitionerCatching;
  final StepInfoPrm stepInfo;
  final StepParameterInitProfile stepParameterInitProfile;

  boolean isTransitionerCatching(FunctionCallContext transitFunctionCall) {
    return transitFunctionCall.functionParameters.stream()
        .anyMatch(
            prm ->
                prm.getFunctionParameterType().equals(ParameterType.TRANSIT_IN_RET_OR_EXCEPTION));
  }

  public StepContext(
      final FunctionCallContext stepFunctionCall,
      final FunctionCallContext transitFunctionCall,
      @Nullable String stepReturnValueToFlowParameterName,
      StateAccessConfig flowStateAccessConfig,
      String stepName,
      String transitName,
      List<InternalTransition> transitions,
      boolean isFirstStep,
      List<Pair<String, String>> flowFactories,
      List<Pair<String, String>> flowRepos
  ) {
    this.stepFunctionCall = stepFunctionCall;
    this.transitFunctionCall = transitFunctionCall;
    this.stepReturnValueToFlowParameterName = stepReturnValueToFlowParameterName;
    this.flowStateAccessConfig = flowStateAccessConfig;
    this.isTransitionerCatching = isTransitionerCatching(transitFunctionCall);
    this.stepInfo = new StepInfo(stepName, transitName, isFirstStep, false);
    this.transitions = transitions;
    this.flowFactories = flowFactories;
    this.flowRepos = flowRepos;
    this.stepParameterInitProfile =
        new StepParameterInitProfile(
            stepFunctionCall, transitFunctionCall, stepReturnValueToFlowParameterName);
  }

  CompletableFuture<?> invokeStepFunction(
      FlowId flowId,
      StateAccess stateAccess,
      EventRunner eventRunner,
      EventContext eventContext,
      EventParametersProvider eventParametersProvider,
      @Nullable FlowEventBus eventBus) {
    StepFunctionCallState stepCallState =
        new StepFunctionCallState(stateAccess, stepReturnValueToFlowParameterName, eventBus);

    CompletableFuture<Object> stepResult =
        FuturesTool.transformAsync(
            eventRunner.runEvents(
                EventType.BEFORE_EXEC, eventContext, eventParametersProvider, null, null),
            void_ ->
                FuturesTool.callAsync(
                    () -> FunctionCallUtil.invokeStepFunction(flowId, stepCallState, stepFunctionCall)),
            directExecutor());
    return FuturesTool.transformAsync(
        stepResult,
        ret ->
            FuturesTool.transform(
                eventRunner.runEvents(
                    EventType.AFTER_EXEC, eventContext, eventParametersProvider, null, null),
                void_ -> ret,
                directExecutor()),
        directExecutor());
  }

  CompletableFuture<InternalTransition> invokeTransitFunction(
      StateAccess stateAccess,
      Object stepFunctionReturnValue,
      EventRunner eventRunner,
      EventContext eventContext,
      EventParametersProvider eventParametersProvider,
      @Nullable FlowEventBus eventBus) {
    TransitFunctionCallState callState =
        new TransitFunctionCallState(stateAccess, stepFunctionReturnValue, eventBus);

    CompletableFuture<InternalTransition> transitResult =
        FuturesTool.transformAsync(
            eventRunner.runEvents(
                EventType.BEFORE_TRANSIT, eventContext, eventParametersProvider, null, null),
            void_ ->
                FuturesTool.callAsync(
                    () -> FunctionCallUtil.invokeTransitFunction(callState, transitFunctionCall)),
            directExecutor());
    return FuturesTool.transformAsync(
        transitResult,
        transition ->
            FuturesTool.transform(
                eventRunner.runEvents(
                    EventType.AFTER_TRANSIT,
                    eventContext,
                    eventParametersProvider,
                    transition,
                    null),
                void_ -> transition,
                directExecutor()),
        directExecutor());
  }

  CompletableFuture<InternalTransition> invokeTransitFunctionOnException(
      StateAccess stateAccess,
      Throwable t,
      EventRunner eventRunner,
      EventContext eventContext,
      EventParametersProvider eventParametersProvider,
      @Nullable FlowEventBus eventBus) {
    if (isTransitionerCatching) {
      TransitFunctionExceptionCallState callState =
          new TransitFunctionExceptionCallState(stateAccess, t, eventBus);

      CompletableFuture<Void> beforeTransit =
          FuturesTool.transformAsync(
              eventRunner.runEvents(
                  EventType.AFTER_EXEC, eventContext, eventParametersProvider, null, null),
              void_ ->
                  eventRunner.runEvents(
                      EventType.BEFORE_TRANSIT, eventContext, eventParametersProvider, null, null),
              directExecutor());
      CompletableFuture<InternalTransition> transitResult =
          FuturesTool.transformAsync(
              beforeTransit,
              void_ ->
                  FuturesTool.callAsync(
                      () ->
                          FunctionCallUtil.invokeTransitFunctionOnException(
                              callState, transitFunctionCall)),
              directExecutor());
      return FuturesTool.transformAsync(
          transitResult,
          transition ->
              FuturesTool.transform(
                  eventRunner.runEvents(
                      EventType.AFTER_TRANSIT,
                      eventContext,
                      eventParametersProvider,
                      transition,
                      null),
                  void_ -> transition,
                  directExecutor()),
          directExecutor());
    } else return CompletableFuture.failedFuture(t);
  }

  @Override
  public StepInfoPrm getStepInfo() {
    return stepInfo;
  }

  @Override
  public List<InternalTransition> getTransitions() {
    return transitions;
  }

  @Override
  public List<Pair<String, String>> getFlowFactories() { return flowFactories; }

  @Override
  public List<Pair<String, String>> getFlowRepos() { return flowRepos; }

  static class RetValAndTransition {
    final Object value;
    final boolean useTransition;

    public RetValAndTransition(Object value, boolean useTransition) {
      this.value = value;
      this.useTransition = useTransition;
    }
  }

  public CompletableFuture<InternalTransition> call(
      FlowId flowId,
      Object flowState,
      EventRunner eventRunner,
      EventContext eventContext,
      EventParametersProvider eventParametersProvider,
      @Nullable FlowEventBus eventBus) {
    final ObjectStateAccess stateAccess = new ObjectStateAccess(flowState, flowStateAccessConfig);

    // 1) Call executor - step function
    CompletableFuture<RetValAndTransition> retValAndTransitionFuture =
        FuturesTool.catchingAsync(
            FuturesTool.transform(
                invokeStepFunction(
                    flowId, stateAccess, eventRunner, eventContext, eventParametersProvider, eventBus),
                stepFunctionReturnValue -> new RetValAndTransition(stepFunctionReturnValue, false),
                directExecutor()),
            Throwable.class,
            t ->
                FuturesTool.transform(
                    invokeTransitFunctionOnException(
                        stateAccess, t, eventRunner, eventContext, eventParametersProvider, eventBus),
                    transition -> new RetValAndTransition(transition, true),
                    directExecutor()),
            directExecutor());
    return FuturesTool.transformAsync(
        retValAndTransitionFuture,
        retValAndTransition -> {
          if (retValAndTransition.useTransition) {
            return CompletableFuture.completedFuture(
                (InternalTransition) retValAndTransition.value);
          } else {
            return invokeTransitFunction(
                stateAccess,
                retValAndTransition.value,
                eventRunner,
                eventContext,
                eventParametersProvider,
                eventBus);
          }
        },
        directExecutor());
  }

  @Override
  public boolean usesEventBus() {
    return stepFunctionCall.hasParameterOfType(ParameterType.EVENT_BUS)
        || transitFunctionCall.hasParameterOfType(ParameterType.EVENT_BUS);
  }

  @Override
  public StepParameterInitProfile stepParameterInitProfile() {
    return stepParameterInitProfile;
  }
}
