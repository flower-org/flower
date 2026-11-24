package com.flower.engine.runner.callfunc;

import com.flower.engine.runner.state.StateAccess;
import com.flower.eventbus.FlowEventBus;
import javax.annotation.Nullable;

public class TransitFunctionCallState {
  public final StateAccess stateAccess;
  public final Object previousReturnValue;
  /** Null if the Flow type doesn't use @EventBus. */
  public final @Nullable FlowEventBus eventBus;

  public TransitFunctionCallState(
      StateAccess stateAccess, Object previousReturnValue, @Nullable FlowEventBus eventBus) {
    this.stateAccess = stateAccess;
    this.previousReturnValue = previousReturnValue;
    this.eventBus = eventBus;
  }
}
