package com.flower.anno.params.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Flow event bus parameter.
 *
 * <p>Get hold of the {@link com.flower.eventbus.FlowEventBus} of the current Flow instance. Each
 * Flow instance has its own bus, shared by all of its step and transit functions for the lifetime of
 * the Flow, e.g. a step can register futures and a later step can wait on {@code next()}.
 *
 * <p>Can be used in Step, Transit and StepAndTransit functions (and GlobalFunctions called from
 * them), but not in Event functions.
 *
 * <p>Parameter must be of type com.flower.eventbus.FlowEventBus
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER})
public @interface EventBus {
  /**
   * Function parameter name. Recommended to specify explicitly, since parameter names via
   * reflection look like (arg0, arg1, arg2, ...) in release builds. If not specified, reflection
   * will be used to get a corresponding parameter name.
   *
   * @return Parameter name
   */
  String name() default "";
}
