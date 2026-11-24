package com.flower.engine.runner.step;

import com.flower.anno.event.EventType;
import com.flower.conf.FlowId;
import com.flower.conf.StepInfoPrm;
import com.flower.engine.function.ParameterType;
import com.flower.engine.runner.callfunc.FunctionCallContext;
import com.flower.engine.runner.callfunc.FunctionCallUtil;
import com.flower.engine.runner.callfunc.FunctionCallState;
import com.flower.engine.runner.event.EventContext;
import com.flower.engine.runner.event.EventParametersProvider;
import com.flower.engine.runner.event.EventRunner;
import com.flower.engine.runner.event.StepInfo;
import com.flower.engine.runner.state.ObjectStateAccess;
import com.flower.engine.runner.state.StateAccessConfig;
import com.flower.eventbus.FlowEventBus;
import com.flower.utilities.FuturesTool;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import javax.annotation.Nullable;
import java.util.concurrent.Executor;

public class StepAndTransitContext implements StepCallContext {
  final FunctionCallContext stepAndTransitFunctionCall;

  final StateAccessConfig flowStateAccessConfig;

  final StepInfoPrm stepInfo;

  final List<InternalTransition> transitions;
  final List<Pair<String, String>> flowFactories;
  final List<Pair<String, String>> flowRepos;
  final StepParameterInitProfile stepParameterInitProfile;

  public StepAndTransitContext(
      FunctionCallContext stepAndTransitFunctionCall,
      StateAccessConfig flowStateAccessConfig,
      String stepName,
      List<InternalTransition> transitions,
      boolean isFirstStep,
      List<Pair<String, String>> flowFactories,
      List<Pair<String, String>> flowRepos) {
    this.stepAndTransitFunctionCall = stepAndTransitFunctionCall;
    this.flowStateAccessConfig = flowStateAccessConfig;
    // exec is the same as transit
    this.stepInfo = new StepInfo(stepName, stepName, isFirstStep, true);
    this.transitions = transitions;
    this.flowFactories = flowFactories;
    this.flowRepos = flowRepos;

    this.stepParameterInitProfile = new StepParameterInitProfile(stepAndTransitFunctionCall);
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

  public CompletableFuture<InternalTransition> call(
      FlowId flowId,
      Object flowState,
      EventRunner eventRunner,
      EventContext eventContext,
      EventParametersProvider eventParametersProvider,
      @Nullable FlowEventBus eventBus,
      Executor functionExecutor) {
    ObjectStateAccess stateAccess = new ObjectStateAccess(flowState, flowStateAccessConfig);

    FunctionCallState callState =
        FunctionCallState.forStepAndTransit(flowId, stateAccess, eventBus);

    CompletableFuture<InternalTransition> stepAndTransitResult =
        FuturesTool.transformAsync(
            eventRunner.runEvents(
                EventType.BEFORE_EXEC, eventContext, eventParametersProvider, null, null),
            void_ ->
                FuturesTool.callAsync(
                    () ->
                        FunctionCallUtil.invokeTransitFunction(
                            callState, stepAndTransitFunctionCall)),
            functionExecutor);
    CompletableFuture<InternalTransition> afterExec =
        FuturesTool.transformAsync(
            stepAndTransitResult,
            ret ->
                FuturesTool.transform(
                    eventRunner.runEvents(
                        EventType.AFTER_EXEC, eventContext, eventParametersProvider, null, null),
                    void_ -> ret,
                    functionExecutor),
            functionExecutor);
    CompletableFuture<InternalTransition> beforeTransit =
        FuturesTool.transformAsync(
            afterExec,
            ret ->
                FuturesTool.transform(
                    eventRunner.runEvents(
                        EventType.BEFORE_TRANSIT,
                        eventContext,
                        eventParametersProvider,
                        null,
                        null),
                    void_ -> ret,
                    functionExecutor),
            functionExecutor);
    return FuturesTool.transformAsync(
        beforeTransit,
        ret ->
            FuturesTool.transform(
                eventRunner.runEvents(
                    EventType.AFTER_TRANSIT, eventContext, eventParametersProvider, ret, null),
                void_ -> ret,
                functionExecutor),
        functionExecutor);
  }

  @Override
  public boolean usesEventBus() {
    return stepAndTransitFunctionCall.hasParameterOfType(ParameterType.EVENT_BUS);
  }

  @Override
  public StepParameterInitProfile stepParameterInitProfile() {
    return stepParameterInitProfile;
  }
}
