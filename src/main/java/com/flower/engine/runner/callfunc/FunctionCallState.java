package com.flower.engine.runner.callfunc;

import com.flower.anno.event.EventType;
import com.flower.conf.FlowId;
import com.flower.conf.Transition;
import com.flower.engine.function.ParameterType;
import com.flower.engine.runner.event.EventParametersProvider;
import com.flower.engine.runner.state.StateAccess;
import com.flower.eventbus.FlowEventBus;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.EnumSet;
import javax.annotation.Nullable;

/**
 * Everything a single function call can inject into its parameters. Which fields are set depends on
 * the {@link Kind} of function; create instances with the {@code forXxx} factory methods.
 */
public class FunctionCallState {
  /** Kind of function being called, and the parameter types it supports. */
  public enum Kind {
    STEP(
        "step",
        EnumSet.of(
            ParameterType.IN,
            ParameterType.OUT,
            ParameterType.IN_OUT,
            ParameterType.EXEC,
            ParameterType.EVENT_BUS,
            ParameterType.FLOW_REPO,
            ParameterType.CHILD_FLOW_FACTORY_REF)),
    TRANSIT(
        "transit",
        EnumSet.of(
            ParameterType.IN,
            ParameterType.OUT,
            ParameterType.IN_OUT,
            ParameterType.EXEC,
            ParameterType.EVENT_BUS,
            ParameterType.STEP_REF,
            ParameterType.TERMINAL,
            ParameterType.TRANSIT_IN_RET,
            ParameterType.TRANSIT_IN_RET_OR_EXCEPTION)),
    /** Transit function called with the exception raised by its step function. */
    TRANSIT_ON_EXCEPTION(
        "transit",
        EnumSet.of(
            ParameterType.IN,
            ParameterType.OUT,
            ParameterType.IN_OUT,
            ParameterType.EXEC,
            ParameterType.EVENT_BUS,
            ParameterType.STEP_REF,
            ParameterType.TERMINAL,
            ParameterType.TRANSIT_IN_RET_OR_EXCEPTION)),
    STEP_AND_TRANSIT(
        "step-and-transit",
        EnumSet.of(
            ParameterType.IN,
            ParameterType.OUT,
            ParameterType.IN_OUT,
            ParameterType.EXEC,
            ParameterType.EVENT_BUS,
            ParameterType.FLOW_REPO,
            ParameterType.CHILD_FLOW_FACTORY_REF,
            ParameterType.STEP_REF,
            ParameterType.TERMINAL)),
    EVENT(
        "event",
        EnumSet.of(
            ParameterType.IN,
            ParameterType.OUT,
            ParameterType.IN_OUT,
            ParameterType.EXEC,
            ParameterType.IN_FROM_FLOW,
            ParameterType.FLOW_INFO,
            ParameterType.STEP_INFO,
            ParameterType.TRANSITION_INFO,
            ParameterType.EVENT_INFO,
            ParameterType.FLOW_EXCEPTION));

    final String label;
    final ImmutableSet<ParameterType> supportedParameterTypes;

    Kind(String label, EnumSet<ParameterType> supportedParameterTypes) {
      this.label = label;
      this.supportedParameterTypes = Sets.immutableEnumSet(supportedParameterTypes);
    }

    public boolean supports(ParameterType parameterType) {
      return supportedParameterTypes.contains(parameterType);
    }
  }

  public final Kind kind;

  /** State that @In / @Out / @InOut refer to: Flow state, or EventProfile state for Events. */
  public final StateAccess stateAccess;

  /** Step / StepAndTransit: needed for child flow factories. */
  @Nullable public final FlowId flowId;

  /** Step: Flow state field the function's return value is written to, if any. */
  @Nullable public final String returnValueToFlowParameterName;

  /** Step / Transit / StepAndTransit: null if the Flow type doesn't use @EventBus. */
  @Nullable public final FlowEventBus eventBus;

  /** Transit: value returned by the step function. */
  @Nullable public final Object stepReturnValue;

  /** Transit on exception: exception raised by the step function. */
  @Nullable public final Throwable stepException;

  /** Event: Flow state, for @InFromFlow. */
  @Nullable public final StateAccess flowStateAccess;

  @Nullable public final EventParametersProvider eventParametersProvider;
  @Nullable public final EventType eventType;
  @Nullable public final Transition transition;
  @Nullable public final Throwable flowException;

  private FunctionCallState(
      Kind kind,
      StateAccess stateAccess,
      @Nullable FlowId flowId,
      @Nullable String returnValueToFlowParameterName,
      @Nullable FlowEventBus eventBus,
      @Nullable Object stepReturnValue,
      @Nullable Throwable stepException,
      @Nullable StateAccess flowStateAccess,
      @Nullable EventParametersProvider eventParametersProvider,
      @Nullable EventType eventType,
      @Nullable Transition transition,
      @Nullable Throwable flowException) {
    this.kind = kind;
    this.stateAccess = stateAccess;
    this.flowId = flowId;
    this.returnValueToFlowParameterName = returnValueToFlowParameterName;
    this.eventBus = eventBus;
    this.stepReturnValue = stepReturnValue;
    this.stepException = stepException;
    this.flowStateAccess = flowStateAccess;
    this.eventParametersProvider = eventParametersProvider;
    this.eventType = eventType;
    this.transition = transition;
    this.flowException = flowException;
  }

  public static FunctionCallState forStep(
      FlowId flowId,
      StateAccess flowStateAccess,
      @Nullable String returnValueToFlowParameterName,
      @Nullable FlowEventBus eventBus) {
    return new FunctionCallState(
        Kind.STEP,
        flowStateAccess,
        flowId,
        returnValueToFlowParameterName,
        eventBus,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  public static FunctionCallState forTransit(
      StateAccess flowStateAccess,
      @Nullable Object stepReturnValue,
      @Nullable FlowEventBus eventBus) {
    return new FunctionCallState(
        Kind.TRANSIT,
        flowStateAccess,
        null,
        null,
        eventBus,
        stepReturnValue,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  public static FunctionCallState forTransitOnException(
      StateAccess flowStateAccess, Throwable stepException, @Nullable FlowEventBus eventBus) {
    return new FunctionCallState(
        Kind.TRANSIT_ON_EXCEPTION,
        flowStateAccess,
        null,
        null,
        eventBus,
        null,
        stepException,
        null,
        null,
        null,
        null,
        null);
  }

  public static FunctionCallState forStepAndTransit(
      FlowId flowId, StateAccess flowStateAccess, @Nullable FlowEventBus eventBus) {
    return new FunctionCallState(
        Kind.STEP_AND_TRANSIT,
        flowStateAccess,
        flowId,
        null,
        eventBus,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  public static FunctionCallState forEvent(
      StateAccess flowStateAccess,
      StateAccess eventProfileStateAccess,
      EventParametersProvider eventParametersProvider,
      EventType eventType,
      @Nullable Transition transition,
      @Nullable Throwable flowException) {
    return new FunctionCallState(
        Kind.EVENT,
        eventProfileStateAccess,
        null,
        null,
        null,
        null,
        null,
        flowStateAccess,
        eventParametersProvider,
        eventType,
        transition,
        flowException);
  }
}
