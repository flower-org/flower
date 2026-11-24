package com.flower.engine.runner.callfunc;

import com.flower.engine.runner.state.StateAccess;
import com.flower.eventbus.FlowEventBus;
import javax.annotation.Nullable;

public class TransitFunctionExceptionCallState {
  public final StateAccess stateAccess;
  public final Throwable stepFunctionException;
  /** Null if the Flow type doesn't use @EventBus. */
  public final @Nullable FlowEventBus eventBus;

  public TransitFunctionExceptionCallState(
      StateAccess stateAccess, Throwable stepFunctionException, @Nullable FlowEventBus eventBus) {
    this.stateAccess = stateAccess;
    this.stepFunctionException = stepFunctionException;
    this.eventBus = eventBus;
  }
}
