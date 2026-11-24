package com.flower.engine.configuration;

import com.flower.anno.flow.State;

import javax.annotation.Nullable;
import java.lang.reflect.Field;

public class StateFieldRecord {
  public Class<?> stateFieldClass;
  public final Field stateField;
  final State annotation;
  public final String stateFieldName;
  public final boolean isFinal;
  public final boolean isNullable;
  /** Declared as @State(initialized = true). */
  public final boolean isDeclaredInitialized;

  public StateFieldRecord(
      Class<?> stateFieldClass,
      Field stateField,
      State annotation,
      String stateFieldName,
      boolean isFinal,
      boolean isNullable) {
    this.stateFieldClass = stateFieldClass;
    this.stateField = stateField;
    this.annotation = annotation;
    this.stateFieldName = stateFieldName;
    this.isFinal = isFinal;
    this.isNullable = isNullable;
    this.isDeclaredInitialized = annotation.initialized();
  }

  /**
   * Whether Flower's null-safety check treats the field as initialized when the Flow / EventProfile
   * instance is created: non-@Nullable, and final, primitive or declared @State(initialized = true).
   */
  public boolean isInitializedOnCreation() {
    return !isNullable
        && (isFinal || stateField.getType().isPrimitive() || isDeclaredInitialized);
  }
}
