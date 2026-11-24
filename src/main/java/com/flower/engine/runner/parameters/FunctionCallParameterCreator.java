package com.flower.engine.runner.parameters;

import com.flower.anno.event.EventType;
import com.flower.anno.params.common.EventBus;
import com.flower.anno.params.common.Exec;
import com.flower.anno.params.common.In;
import com.flower.anno.params.common.InOut;
import com.flower.anno.params.common.Out;
import com.flower.anno.params.events.EventInfo;
import com.flower.anno.params.events.FlowException;
import com.flower.anno.params.events.FlowInfo;
import com.flower.anno.params.events.InFromFlow;
import com.flower.anno.params.events.StepInfo;
import com.flower.anno.params.events.TransitionInfo;
import com.flower.anno.params.step.FlowFactory;
import com.flower.anno.params.step.FlowRepo;
import com.flower.anno.params.transit.InRet;
import com.flower.anno.params.transit.InRetOrException;
import com.flower.anno.params.transit.StepRef;
import com.flower.anno.params.transit.Terminal;
import com.flower.conf.FlowInfoPrm;
import com.flower.conf.StepInfoPrm;
import com.flower.conf.Transition;
import com.flower.engine.configuration.FlowTypeRecord;
import com.flower.engine.configuration.FunctionParameterRecord;
import com.flower.engine.configuration.FunctionReturnValueRecord;
import com.flower.engine.configuration.TransitParameterOverrideRecord;
import com.flower.engine.function.ParameterType;
import com.flower.engine.runner.FlowRunner;
import com.flower.engine.runner.state.StateAccessConfig;
import com.flower.engine.runner.step.InternalTransition;
import com.flower.eventbus.FlowEventBus;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;

public class FunctionCallParameterCreator {
  final FlowRunner flowRunner;

  /** Parameter creator for each Flower parameter annotation. */
  final ImmutableMap<Class<? extends Annotation>, ParameterCreator> creators;

  /** Annotations that refer to EventProfile state when used in Event functions. */
  static final ImmutableSet<Class<? extends Annotation>> EVENT_PROFILE_STATE_ANNOTATIONS =
      ImmutableSet.of(In.class, Out.class, InOut.class);

  public FunctionCallParameterCreator(FlowRunner flowRunner) {
    this.flowRunner = flowRunner;

    InParameterCreator inParameterCreator = new InParameterCreator();
    StepRefOrTerminalParameterCreator stepRefOrTerminalParameterCreator =
        new StepRefOrTerminalParameterCreator();
    creators =
        ImmutableMap.<Class<? extends Annotation>, ParameterCreator>builder()
            .put(In.class, inParameterCreator)
            .put(InFromFlow.class, inParameterCreator)
            .put(Out.class, new OutParameterCreator())
            .put(InOut.class, new InOutParameterCreator())
            .put(InRet.class, new InRetParameterCreator())
            .put(InRetOrException.class, new InRetOrExceptionParameterCreator())
            .put(StepRef.class, stepRefOrTerminalParameterCreator)
            .put(Terminal.class, stepRefOrTerminalParameterCreator)
            .put(FlowFactory.class, new FlowTypeFactoryParameterCreator(flowRunner))
            .put(FlowRepo.class, new FlowRepoParameterCreator(flowRunner))
            .put(
                Exec.class,
                new FixedTypeParameterCreator(
                    ParameterType.EXEC, Executor.class, flowRunner.getScheduler()))
            .put(
                EventBus.class,
                new FixedTypeParameterCreator(ParameterType.EVENT_BUS, FlowEventBus.class, null))
            .put(
                EventInfo.class,
                new FixedTypeParameterCreator(ParameterType.EVENT_INFO, EventType.class, null))
            .put(
                FlowInfo.class,
                new FixedTypeParameterCreator(ParameterType.FLOW_INFO, FlowInfoPrm.class, null))
            .put(
                StepInfo.class,
                new FixedTypeParameterCreator(ParameterType.STEP_INFO, StepInfoPrm.class, null))
            .put(
                TransitionInfo.class,
                new FixedTypeParameterCreator(
                    ParameterType.TRANSITION_INFO, Transition.class, null))
            .put(
                FlowException.class,
                new FixedTypeParameterCreator(ParameterType.FLOW_EXCEPTION, Throwable.class, null))
            .build();
  }

  public ParameterCreationResult createFunctionCallParameter(
      FlowTypeRecord flowTypeRecord,
      @Nullable Class<?> functionFlowType,
      @Nullable Method functionOrCallMethod,
      String functionOrCallName,
      @Nullable Method stepFunctionMethod,
      @Nullable FunctionReturnValueRecord stepReturnValueRecord,
      @Nullable Method globalFunctionMethod,
      final FunctionParameterRecord functionParameter,
      StateAccessConfig flowStateAccess,
      @Nullable StateAccessConfig eventProfileStateAccess,
      @Nullable FunctionParameterRecord parameterOverrideFromCall,
      @Nullable TransitParameterOverrideRecord transitParameterOverride,
      @Nullable Type genericInRetType,
      List<InternalTransition> stepRefPrms,
      List<Pair<String, String>> flowFactories,
      List<Pair<String, String>> flowRepos) {
    Class<? extends Annotation> annotationType = functionParameter.annotation.annotationType();
    ParameterCreator parameterCreator = creators.get(annotationType);
    if (parameterCreator == null) {
      throw new IllegalStateException(
          String.format(
              "Parameter must be annotated with one of [@In, @Out, @InOut, @Exec, @EventBus, @StepRef, @Terminal, @InRet, @InRetOrException, @FlowFactory, @FlowRepo, @InFromFlow, @EventInfo, @FlowInfo, @StepInfo, @TransitionInfo, @FlowException]. Flow [%s] ParameterName [%s]",
              flowTypeRecord.flowTypeName, functionParameter.name));
    }

    // @In / @Out / @InOut in Event functions refer to EventProfile state
    StateAccessConfig stateAccessConfig =
        eventProfileStateAccess != null && EVENT_PROFILE_STATE_ANNOTATIONS.contains(annotationType)
            ? eventProfileStateAccess
            : flowStateAccess;

    return parameterCreator.createParameter(
        flowTypeRecord,
        functionFlowType,
        functionOrCallMethod,
        functionOrCallName,
        stepFunctionMethod,
        stepReturnValueRecord,
        globalFunctionMethod,
        functionParameter,
        stateAccessConfig,
        parameterOverrideFromCall,
        transitParameterOverride,
        genericInRetType,
        stepRefPrms,
        flowFactories,
        flowRepos);
  }
}
