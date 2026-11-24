package com.flower.engine.runner.callfunc;

import com.flower.engine.runner.state.StateAccess;
import com.flower.eventbus.FlowEventBus;
import javax.annotation.Nullable;

public class StepAndTransitFunctionCallState {
  public final StateAccess stateAccess;
  /** Null if the Flow type doesn't use @EventBus. */
  public final @Nullable FlowEventBus eventBus;

  public StepAndTransitFunctionCallState(StateAccess stateAccess, @Nullable FlowEventBus eventBus) {
    this.stateAccess = stateAccess;
    this.eventBus = eventBus;
  }
}
