package com.flower.engine.runner.event;

import com.flower.engine.runner.state.ObjectStateAccess;

public class EventFunctionContext {
  public final ObjectStateAccess eventProfileStateAccess;
  public final ObjectStateAccess flowStateAccess;
  public final EventFunction eventFunction;
  public final String eventProfileName;

  public EventFunctionContext(
      ObjectStateAccess eventProfileStateAccess,
      ObjectStateAccess flowStateAccess,
      EventFunction eventFunction,
      String eventProfileName) {
    this.eventProfileName = eventProfileName;
    this.eventProfileStateAccess = eventProfileStateAccess;
    this.flowStateAccess = flowStateAccess;
    this.eventFunction = eventFunction;
  }
}
