package com.flower.engine.runner.step;

import com.flower.conf.Transition;

import java.time.Duration;
import javax.annotation.Nullable;

public class TransitionImpl implements InternalTransition {
  private final boolean isTerminal;
  @Nullable private final String stepName;
  @Nullable private final String note;
  @Nullable private final String parameterName;
  @Nullable private final Duration delay;

  private TransitionImpl(String stepName, @Nullable String note, @Nullable String parameterName) {
    this.isTerminal = false;
    this.stepName = stepName;
    this.delay = null;
    this.note = note;
    this.parameterName = parameterName;
  }

  private TransitionImpl(@Nullable String note, @Nullable String parameterName) {
    this.isTerminal = true;
    this.stepName = null;
    this.delay = null;
    this.note = note;
    this.parameterName = parameterName;
  }

  private TransitionImpl(TransitionImpl clone, @Nullable Duration newDelay) {
    this.stepName = clone.stepName;
    this.isTerminal = clone.isTerminal;
    this.delay = newDelay;
    this.note = clone.getNote();
    this.parameterName = clone.getParameterName();
  }

  @Nullable
  @Override
  public String getStepName() {
    return stepName;
  }

  @Override
  public boolean isTerminal() {
    return isTerminal;
  }

  @Nullable
  @Override
  public String getNote() { return note; }

  @Nullable
  @Override
  public String getParameterName() {
    return parameterName;
  }

  @Nullable
  @Override
  public Duration getDelay() {
    return delay;
  }

  @Override
  public Transition setDelay(@Nullable Duration delay) {
    return new TransitionImpl(this, delay);
  }

  @Override
  public String toString() {
    return "TransitionImpl{"
        + "isTerminal="
        + isTerminal
        + ", stepName='"
        + stepName
        + '\''
        + ", delay="
        + delay
        + '}';
  }

  // pre-hashing all possible steps and returning them from Immutable Hash Map is redundant,
  // because all steps are effectively static - created once on init and reused in form of
  // parameter's SpecialObjects
  public static InternalTransition getStepTransition(String stepName, @Nullable String note) {
    return getStepTransition(stepName, note, null);
  }

  public static InternalTransition getStepTransition(
      String stepName, @Nullable String note, @Nullable String parameterName) {
    return new TransitionImpl(stepName, note, parameterName);
  }

  public static InternalTransition getTerminalTransition(@Nullable String note) {
    return getTerminalTransition(note, null);
  }

  public static InternalTransition getTerminalTransition(@Nullable String note, @Nullable String parameterName) {
    return new TransitionImpl(note, parameterName);
  }
}
