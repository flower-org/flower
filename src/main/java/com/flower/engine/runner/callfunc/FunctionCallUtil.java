package com.flower.engine.runner.callfunc;

import com.flower.anno.params.common.Output;
import com.flower.conf.FactoryOfFlowTypeFactories;
import com.flower.engine.function.FlowerInOutPrm;
import com.flower.engine.function.FlowerOutPrm;
import com.flower.engine.function.FlowerRetValOrException;
import com.flower.engine.function.FunctionCallParameter;
import com.flower.engine.function.ParameterType;
import com.flower.engine.runner.state.StateAccess;
import com.flower.engine.runner.step.InternalTransition;
import com.flower.utilities.FuturesTool;
import com.google.common.base.Preconditions;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;

public class FunctionCallUtil {
  // ---------------- INVOCATION ----------------

  public static CompletableFuture<?> invokeStepFunction(
      FunctionCallState callState, FunctionCallContext callContext) {
    return invokeFunction(callState, callContext, null);
  }

  /** Transit, TransitOnException and StepAndTransit functions. */
  @SuppressWarnings("unchecked")
  public static CompletableFuture<InternalTransition> invokeTransitFunction(
      FunctionCallState callState, FunctionCallContext callContext) {
    // No extra stage to cast: their return type (Transition) is validated at initialization
    return (CompletableFuture<InternalTransition>)
        (CompletableFuture<?>) invokeFunction(callState, callContext, null);
  }

  /**
   * Calls an Event function.
   *
   * @param invokeExecutor if not null, the function itself is called on this executor; otherwise
   *     it's called on the current thread. Parameters are always read on the current thread, so
   *     they reflect the state at the time of the event.
   */
  @SuppressWarnings("unchecked")
  public static CompletableFuture<Void> invokeEventFunction(
      FunctionCallState callState,
      FunctionCallContext callContext,
      @Nullable Executor invokeExecutor) {
    // No extra stage to discard the return value: Event functions return void, and the result
    // value is never read
    return (CompletableFuture<Void>)
        (CompletableFuture<?>) invokeFunction(callState, callContext, invokeExecutor);
  }

  /**
   * Calls a function: resolves its parameters, calls it, then updates state with its @Out / @InOut
   * parameters (and return value, if bound to a state field). If the function fails, state is updated
   * partially and the returned future fails.
   *
   * @return future of the function's return value
   */
  static CompletableFuture<Object> invokeFunction(
      FunctionCallState callState,
      FunctionCallContext callContext,
      @Nullable Executor invokeExecutor) {
    // 1) Get Function parameters from State
    List<Object> functionCallParameters = resolveParameters(callState, callContext);

    // 2) Call function: on invokeExecutor if set (lets synchronous functions run concurrently),
    // otherwise directly on the current thread.
    CompletableFuture<?> functionFuture;
    if (invokeExecutor != null) {
      functionFuture =
          FuturesTool.transformAsync(
              CompletableFuture.completedFuture(null),
              void_ ->
                  FuturesTool.callAsync(
                      () -> rawInvokeFunction(callContext.function, functionCallParameters)),
              invokeExecutor);
    } else {
      try {
        functionFuture = rawInvokeFunction(callContext.function, functionCallParameters);
      } catch (Throwable t) {
        functionFuture = CompletableFuture.failedFuture(t);
      }
    }

    // 3) Update State with Output parameters
    return FuturesTool.tryCatchAsync(
        functionFuture,
        functionReturnValue ->
            updateState(
                callState.stateAccess,
                callState.returnValueToFlowParameterName,
                callContext.functionParameters,
                functionCallParameters,
                functionReturnValue,
                callContext.getFlowStepInfo(),
                false),
        Throwable.class,
        t -> {
          CompletableFuture<?> unusedPartialUpdate =
              updateState(
                  callState.stateAccess,
                  callState.returnValueToFlowParameterName,
                  callContext.functionParameters,
                  functionCallParameters,
                  null,
                  callContext.getFlowStepInfo(),
                  true);
          return CompletableFuture.failedFuture(t);
        },
        FuturesTool.directExecutor());
  }

  // ---------------- PARAMETERS ----------------

  static List<Object> resolveParameters(
      FunctionCallState callState, FunctionCallContext functionCallContext) {
    String flowStepInfo = functionCallContext.getFlowStepInfo();
    List<Object> parameters = new ArrayList<>();
    for (FunctionCallParameter callParameter : functionCallContext.functionParameters) {
      ParameterType parameterType = callParameter.getFunctionParameterType();
      // Also validated at initialization, see FunctionRecord
      if (!callState.kind.supports(parameterType)) {
        throw new IllegalStateException(
            String.format(
                flowStepInfo + " Fatal: Function parameter type is not supported in %s functions: [%s]. FlowField [%s] FunctionPrm [%s]",
                callState.kind.label,
                parameterType,
                callParameter.getStateFieldName(),
                callParameter.getFunctionParameterName()));
      }
      parameters.add(resolveParameter(callState, callParameter, flowStepInfo));
    }
    return parameters;
  }

  @Nullable
  static Object resolveParameter(
      FunctionCallState callState, FunctionCallParameter callParameter, String flowStepInfo) {
    switch (callParameter.getFunctionParameterType()) {
      case IN:
      case OUT:
      case IN_OUT:
        return getInOutParameter(callState.stateAccess, callParameter, flowStepInfo);
      case IN_FROM_FLOW:
        return getInOutParameter(
            Preconditions.checkNotNull(callState.flowStateAccess), callParameter, flowStepInfo);
      case EXEC:
      case STEP_REF:
      case TERMINAL:
      case FLOW_REPO:
        return callParameter.getSpecialObject();
      case EVENT_BUS:
        // Created for every Flow whose type has an @EventBus parameter, see FlowCallContext
        return Preconditions.checkNotNull(callState.eventBus);
      case CHILD_FLOW_FACTORY_REF:
        return ((FactoryOfFlowTypeFactories)
                Preconditions.checkNotNull(callParameter.getSpecialObject()))
            .getFactory(Preconditions.checkNotNull(callState.flowId));
      case TRANSIT_IN_RET:
        return callState.stepReturnValue;
      case TRANSIT_IN_RET_OR_EXCEPTION:
        if (callState.kind == FunctionCallState.Kind.TRANSIT_ON_EXCEPTION) {
          Throwable stepException = Preconditions.checkNotNull(callState.stepException);
          return new FlowerRetValOrException<>(stepException);
        } else {
          Object stepReturnValue = callState.stepReturnValue;
          return new FlowerRetValOrException<>(stepReturnValue);
        }
      case FLOW_INFO:
        return Preconditions.checkNotNull(callState.eventParametersProvider).getFlowInfo();
      case STEP_INFO:
        return Preconditions.checkNotNull(callState.eventParametersProvider).getStepInfo();
      case TRANSITION_INFO:
        return callState.transition;
      case EVENT_INFO:
        return callState.eventType;
      case FLOW_EXCEPTION:
        return callState.flowException;
    }
    throw new IllegalStateException("Unknown parameter type " + callParameter.getFunctionParameterType());
  }

  // ---------------- COMMON ----------------

  @Nullable
  static Object getInOutParameter(StateAccess stateAccess, FunctionCallParameter callParameter, String flowStepInfo) {
    ParameterType parameterType = callParameter.getFunctionParameterType();
    Object parameterValue;
    if (parameterType == ParameterType.OUT) parameterValue = new FlowerOutPrm<>();
    else {
      if (parameterType == ParameterType.IN_FROM_FLOW) {
        if (!stateAccess.hasField(Preconditions.checkNotNull(callParameter.getStateFieldName()))) {

          // Defaults for primitive type parameters when Flow State Field doesn't exist
          if (callParameter.getParameterType().equals(byte.class)) parameterValue = (byte) 0;
          else if (callParameter.getParameterType().equals(short.class)) parameterValue = (short) 0;
          else if (callParameter.getParameterType().equals(int.class)) parameterValue = 0;
          else if (callParameter.getParameterType().equals(long.class)) parameterValue = 0L;
          else if (callParameter.getParameterType().equals(float.class)) parameterValue = 0F;
          else if (callParameter.getParameterType().equals(double.class)) parameterValue = 0D;
          else if (callParameter.getParameterType().equals(boolean.class)) parameterValue = false;
          else if (callParameter.getParameterType().equals(char.class)) parameterValue = (char) 0;
          else parameterValue = null;

        } else {
          parameterValue =
              stateAccess.getField(Preconditions.checkNotNull(callParameter.getStateFieldName()));
        }
      } else {
        parameterValue =
            stateAccess.getField(Preconditions.checkNotNull(callParameter.getStateFieldName()));
        if (parameterType == ParameterType.IN_OUT) {
          parameterValue = new FlowerInOutPrm<>(parameterValue);
        }
      }
    }

    if (callParameter.isCheckNotNull()) {
      if (parameterValue == null) {
        throw new IllegalStateException(
            String.format(
                flowStepInfo + " Fatal: Function parameter marked as `CheckNotNull` has value null: [%s]. FlowField [%s] FunctionPrm [%s]",
                parameterType,
                callParameter.getStateFieldName(),
                callParameter.getFunctionParameterName()));
      }
    }
    return parameterValue;
  }

  static CompletableFuture<?> rawInvokeFunction(Method function, List<Object> functionCallParameters)
      throws Exception {
    // Call function
    CompletableFuture<?> retFuture;
    try {
      Object retObj = function.invoke(null, functionCallParameters.toArray());
      retFuture =
          (retObj instanceof CompletableFuture)
              ? (CompletableFuture<?>) retObj
              : CompletableFuture.completedFuture(retObj);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw getUnwrappedException(e);
    }

    return retFuture;
  }

  /**
   * Updates {@code stateAccess} with @Out / @InOut parameters, once their futures (if any) are done,
   * and with the return value if it's bound to a state field.
   *
   * @return future of {@code returnValue}
   */
  static CompletableFuture<Object> updateState(
      StateAccess stateAccess,
      @Nullable String returnValueToFlowParameterName,
      List<FunctionCallParameter> functionParameters,
      List<Object> functionParameterObjects,
      @Nullable Object returnValue,
      String flowStepInfo,
      boolean partial) {
    return FuturesTool.transformAsync(
        waitForOutParams(functionParameters, functionParameterObjects),
        void_ -> {
          try {
            updateState0(
                stateAccess,
                returnValueToFlowParameterName,
                functionParameters,
                functionParameterObjects,
                returnValue,
                flowStepInfo,
                partial);
          } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
          }
          return CompletableFuture.completedFuture(returnValue);
        },
        FuturesTool.directExecutor());
  }

  static CompletableFuture<Void> waitForOutParams(
      List<FunctionCallParameter> functionParameters, List<Object> functionParameterObjects) {
    List<CompletableFuture<?>> futureList = new ArrayList<>();

    int i = 0;
    for (FunctionCallParameter callParameter : functionParameters) {
      ParameterType parameterType = callParameter.getFunctionParameterType();
      if (parameterType == ParameterType.OUT || parameterType == ParameterType.IN_OUT) {
        FlowerOutPrm<?> flowerOutPrm = (FlowerOutPrm<?>) functionParameterObjects.get(i);
        if (flowerOutPrm.getOptFuture().isPresent()) {
          futureList.add(flowerOutPrm.getOptFuture().get());
        }
      }
      i++;
    }

    if (futureList.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    } else {
      return FuturesTool.whenAllCompleteCall(futureList, () -> null, FuturesTool.directExecutor());
    }
  }

  static void updateState0(
      StateAccess stateAccess,
      @Nullable String returnValueToFlowParameterName,
      List<FunctionCallParameter> functionParameters,
      List<Object> functionParameterObjects,
      @Nullable Object returnValue,
      String flowStepInfo,
      boolean partial)
      throws Exception {
    ParameterType outType = ParameterType.OUT;
    ParameterType inOutType = ParameterType.IN_OUT;

    // Update values of Out and InOut Flow fields
    int i = 0;
    for (FunctionCallParameter callParameter : functionParameters) {
      ParameterType parameterType = callParameter.getFunctionParameterType();
      if (parameterType == outType || parameterType == inOutType) {
        FlowerOutPrm<?> flowerOutPrm = (FlowerOutPrm<?>) functionParameterObjects.get(i);
        updateOutParameter(callParameter, flowerOutPrm, stateAccess, flowStepInfo, partial);
      }
      i++;
    }
    if (!partial) {
      // If function's return value is bound to a Flow field, update that field as well
      if (returnValueToFlowParameterName != null && returnValue != null)
        stateAccess.updateField(returnValueToFlowParameterName, returnValue);
    }
  }

  static Exception getUnwrappedException(Exception e) {
    if (e.getCause() != null) {
      Throwable t = e.getCause();
      if (t instanceof Exception) return (Exception) t;
      else return new Exception(t);
    } else return e;
  }

  static void updateOutParameter(
      FunctionCallParameter callParameter, FlowerOutPrm<?> flowerOutPrm, StateAccess stateAccess, String flowStepInfo, boolean partial)
      throws Exception {
    Object value = null;
    if (flowerOutPrm.getOpt().isPresent()) {
      value = flowerOutPrm.getOpt().get();
    } else if (flowerOutPrm.getOptFuture().isPresent()) {
      try {
        // Please note that Future.get() will never block here, because in
        // FunctionCallUtil.updateEventState(...)
        // the call to updateState0(...), which calls this method is preceded by waitForOutParams()
        value = flowerOutPrm.getOptFuture().get().get();
      } catch (Exception e) {
        if (!partial) {
          //Ignore Exceptions for partial updates
          throw getUnwrappedException(e);
        }
      }
    }

    if (value != null) {
      stateAccess.updateField(Preconditions.checkNotNull(callParameter.getStateFieldName()), value);
    } else {
      // TODO: at some point this needs to be changed to compile-level or initialization-level check
      // TODO: to prevent raising this Exception at runtime
      ParameterType parameterType = callParameter.getFunctionParameterType();
      if ((parameterType == ParameterType.OUT || parameterType == ParameterType.IN_OUT)
          && callParameter.getOutput() == Output.MANDATORY && !partial)
        //TODO: inject flow and function name here for better message clarity

        //TODO: should we throw? this can cause NPE potentially
        // BUT when we throw, it can destroy otherwise perfectly fine flow mid flight for no good reason and
        // is VERY hard to catch since it's runtime.
        // My current take on this is that possible future error-prone/null-away style code analysis should take better care of it.
        System.err.println(
            String.format(
                flowStepInfo + " Fatal: value of Out or InOut parameter with Output.MANDATORY wasn't set in the Function call. FlowField [%s] FunctionPrm [%s]",
                callParameter.getStateFieldName(), callParameter.getFunctionParameterName()));
    }
  }
}
