package com.flower.engine.function;

import com.flower.utilities.FlowerException;

import java.lang.reflect.Field;

public class StateField {
  public final Class<?> stateFieldClass;
  public final Field field;
  public final boolean isFinal;
  public final boolean isNullable;
  /** Declared as @State(initialized = true). */
  public final boolean isDeclaredInitialized;

  public StateField(
      Class<?> stateFieldClass,
      Field field,
      boolean isFinal,
      boolean isNullable,
      boolean isDeclaredInitialized) {
    this.stateFieldClass = stateFieldClass;
    this.field = field;
    this.field.setAccessible(true);
    this.isFinal = isFinal;
    this.isNullable = isNullable;
    this.isDeclaredInitialized = isDeclaredInitialized;
  }

  /** See {@code StateFieldRecord.isInitializedOnCreation()}. */
  public boolean isInitializedOnCreation() {
    return !isNullable && (isFinal || field.getType().isPrimitive() || isDeclaredInitialized);
  }

  /** Whether the field's @State(initialized = true) declaration must be verified on creation. */
  public boolean mustBeVerifiedOnCreation() {
    return isDeclaredInitialized && !field.getType().isPrimitive();
  }

  public Object getFieldValue(Object flowState) {
    try {
      return field.get(flowState);
    } catch (IllegalAccessException e) {
      throw new FlowerException(e);
    }
  }

  public void updateFieldValue(Object flowState, Object newValue) {
    try {
      field.set(flowState, newValue);
    } catch (IllegalAccessException e) {
      throw new FlowerException(e);
    }
  }
}
