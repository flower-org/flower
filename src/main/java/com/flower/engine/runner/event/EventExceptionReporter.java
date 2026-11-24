package com.flower.engine.runner.event;

import com.flower.conf.EventExceptionHandler;
import com.flower.conf.EventFunctionFailure;
import java.util.logging.Level;

/** Holds the engine's {@link EventExceptionHandler}, and calls it safely. */
public class EventExceptionReporter {
  private volatile EventExceptionHandler handler = EventExceptionHandler.LOGGING;

  public void setHandler(EventExceptionHandler handler) {
    this.handler = handler;
  }

  /** Reports {@code failure}; never throws, except for fatal JVM errors. */
  public void report(EventFunctionFailure failure) {
    try {
      handler.onEventFunctionFailure(failure);
    } catch (VirtualMachineError e) {
      throw e;
    } catch (Throwable handlerException) {
      try {
        if (handlerException != failure.exception()) {
          handlerException.addSuppressed(failure.exception());
        }
        EventExceptionHandler.LOGGER.log(
            Level.WARNING,
            "EventExceptionHandler failed while reporting: " + failure,
            handlerException);
      } catch (VirtualMachineError e) {
        throw e;
      } catch (Throwable ignored) {
        // Nowhere left to report to
      }
    }
  }
}
