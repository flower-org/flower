package com.flower.engine.runner.callfunc;

import com.flower.engine.runner.state.StateAccess;
import com.flower.eventbus.FlowEventBus;

import javax.annotation.Nullable;

public class StepFunctionCallState {
  public final StateAccess stateAccess;
  public final @Nullable String returnValueToFlowParameterName;
  /** Null if the Flow type doesn't use @EventBus. */
  public final @Nullable FlowEventBus eventBus;

  public StepFunctionCallState(
      StateAccess stateAccess,
      @Nullable String returnValueToFlowParameterName,
      @Nullable FlowEventBus eventBus) {
    this.stateAccess = stateAccess;
    this.returnValueToFlowParameterName = returnValueToFlowParameterName;
    this.eventBus = eventBus;
  }
}
