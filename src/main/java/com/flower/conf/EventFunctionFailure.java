package com.flower.conf;

import com.flower.anno.event.Concurrency;
import com.flower.anno.event.EventType;

/**
 * An exception raised by a NON_BLOCKING, BLOCKING or SYNCHRONIZED Event Function. Such exceptions
 * don't affect the Flow; they're reported to the {@link EventExceptionHandler}.
 */
public final class EventFunctionFailure {
  private final FlowInfoPrm flowInfo;
  private final EventType eventType;
  private final String eventProfileName;
  private final String eventFunctionName;
  private final Concurrency concurrency;
  private final Throwable exception;

  public EventFunctionFailure(
      FlowInfoPrm flowInfo,
      EventType eventType,
      String eventProfileName,
      String eventFunctionName,
      Concurrency concurrency,
      Throwable exception) {
    this.flowInfo = flowInfo;
    this.eventType = eventType;
    this.eventProfileName = eventProfileName;
    this.eventFunctionName = eventFunctionName;
    this.concurrency = concurrency;
    this.exception = exception;
  }

  public FlowInfoPrm flowInfo() {
    return flowInfo;
  }

  public EventType eventType() {
    return eventType;
  }

  public String eventProfileName() {
    return eventProfileName;
  }

  public String eventFunctionName() {
    return eventFunctionName;
  }

  public Concurrency concurrency() {
    return concurrency;
  }

  public Throwable exception() {
    return exception;
  }

  @Override
  public String toString() {
    return String.format(
        "Event Function [%s.%s] (%s) failed on %s of Flow [%s] id [%s]: %s",
        eventProfileName,
        eventFunctionName,
        concurrency,
        eventType,
        flowInfo.flowName(),
        flowInfo.flowId(),
        exception);
  }
}
