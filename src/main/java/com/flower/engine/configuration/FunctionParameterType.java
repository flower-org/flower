package com.flower.engine.configuration;

import com.google.common.collect.ImmutableSet;
import java.util.Set;

/**
 * Flower type of a parameter as <b>declared</b> on a function (Step, Transit, Event, Global function
 * or Call), derived from its parameter annotation. Used at initialization, e.g. to check that a Call's
 * or a transit override's parameters are compatible with the parameters they override (see {@link
 * #isCompatible}).
 *
 * <p>Not to be confused with {@link com.flower.engine.function.ParameterType}, which is the type
 * actually injected at call time, after overrides are applied: e.g. a @StepRef parameter of a Transit
 * function overridden as @Terminal is declared STEP_REF, but injected as TERMINAL.
 */
public enum FunctionParameterType {
  IN,
  OUT,
  IN_OUT,
  EXEC,

  IN_FROM_FLOW,

  IN_RET,
  IN_RET_OR_EXCEPTION,

  STEP_REF,
  TERMINAL,
  FLOW_TYPE_FACTORY,
  FLOW_REPO,
  EVENT_BUS,

  FLOW_INFO,
  STEP_INFO,
  TRANSITION_INFO,
  EVENT_INFO,
  FLOW_EXCEPTION; // ,

  // RUNNING_TIME_STATS;

  static <T> boolean bothIn(T t1, T t2, Set<T> inSet) {
    return inSet.contains(t1) && inSet.contains(t2);
  }

  public static boolean isCompatible(FunctionParameterType type1, FunctionParameterType type2) {
    if (type1 == type2) return true;

    return bothIn(type1, type2, ImmutableSet.of(IN, IN_RET))
        || bothIn(type1, type2, ImmutableSet.of(IN, IN_FROM_FLOW))
        || bothIn(type1, type2, ImmutableSet.of(STEP_REF, TERMINAL));
  }
}
