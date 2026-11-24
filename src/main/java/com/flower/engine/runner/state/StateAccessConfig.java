package com.flower.engine.runner.state;

import com.flower.engine.function.StateField;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Map;
import javax.annotation.Nullable;

public class StateAccessConfig {
  public final Map<String, StateField> stateFieldMap;
  /** Fields declared @State(initialized = true), verified when an instance is created. */
  final ImmutableList<StateField> fieldsToVerifyOnCreation;

  public StateAccessConfig(Map<String, StateField> stateFieldMap) {
    this.stateFieldMap = stateFieldMap;
    this.fieldsToVerifyOnCreation =
        stateFieldMap.values().stream()
            .filter(StateField::mustBeVerifiedOnCreation)
            .collect(ImmutableList.toImmutableList());
  }

  /**
   * Verifies that fields declared @State(initialized = true) are non-null in a newly created Flow or
   * EventProfile instance.
   *
   * @param description e.g. "Flow [MyFlow]", for the error message
   */
  public void verifyInitializedOnCreation(Object state, String description) {
    for (StateField field : fieldsToVerifyOnCreation) {
      if (field.getFieldValue(state) == null) {
        throw new IllegalStateException(
            String.format(
                "State field [%s] of %s is declared @State(initialized = true), but it's null. Initialize it (e.g. with a field initializer or in every constructor), or remove initialized = true.",
                field.field.getName(), description));
      }
    }
  }

  @Nullable
  public StateField getFieldRecord(String fieldName) {
    return stateFieldMap.get(fieldName);
  }

  public Object getField(Object flowState, String fieldName) {
    return Preconditions.checkNotNull(stateFieldMap.get(fieldName)).getFieldValue(flowState);
  }

  public boolean hasField(String fieldName) {
    return stateFieldMap.containsKey(fieldName);
  }

  public void updateField(Object flowState, String fieldName, Object parameterValue) {
    Preconditions.checkNotNull(stateFieldMap.get(fieldName))
        .updateFieldValue(flowState, parameterValue);
  }
}
