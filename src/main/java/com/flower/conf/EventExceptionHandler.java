package com.flower.conf;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Receives exceptions raised by NON_BLOCKING, BLOCKING and SYNCHRONIZED Event Functions, which
 * don't affect the Flow (FINALIZER exceptions fail the Flow instead, and aren't reported here). Set
 * with {@code Flower.setEventExceptionHandler(...)}; defaults to {@link #LOGGING}.
 *
 * <p>Called on a Flower scheduler thread, so it should be fast and non-blocking. If it throws, the
 * exception is logged via {@link #LOGGER} and otherwise ignored; it never affects the Flow.
 */
@FunctionalInterface
public interface EventExceptionHandler {
  /** Logger used by {@link #LOGGING}, and for exceptions raised by handlers themselves. */
  Logger LOGGER = Logger.getLogger("com.flower.events");

  /** Default handler: logs the failure at WARNING via java.util.logging. */
  EventExceptionHandler LOGGING =
      failure -> LOGGER.log(Level.WARNING, failure.toString(), failure.exception());

  void onEventFunctionFailure(EventFunctionFailure failure);
}
