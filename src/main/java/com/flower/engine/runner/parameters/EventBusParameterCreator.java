package com.flower.engine.runner.parameters;

import com.flower.engine.configuration.FlowTypeRecord;
import com.flower.engine.configuration.FunctionParameterRecord;
import com.flower.engine.configuration.FunctionReturnValueRecord;
import com.flower.engine.configuration.TransitParameterOverrideRecord;
import com.flower.engine.function.FunctionCallParameter;
import com.flower.engine.function.ParameterType;
import com.flower.engine.runner.state.StateAccessConfig;
import com.flower.engine.runner.step.InternalTransition;
import com.flower.eventbus.FlowEventBus;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Creates @EventBus parameters. Unlike other special parameters, there's no special object: each
 * Flow instance has its own FlowEventBus, which is resolved when the function is called.
 */
public class EventBusParameterCreator extends ParameterCreator {
  @Override
  public ParameterCreationResult createParameter(
      FlowTypeRecord flowTypeRecord,
      @Nullable Class<?> functionFlowType,
      @Nullable Method functionOrCallMethod,
      String functionOrCallName,
      @Nullable Method stepFunctionMethodForTransitionerReference,
      @Nullable FunctionReturnValueRecord stepFunctionReturnValue,
      @Nullable Method globalFunctionMethod,
      FunctionParameterRecord baseParameter,
      StateAccessConfig stateAccess, // NOT USED
      @Nullable FunctionParameterRecord parameterOverrideFromCall,
      @Nullable TransitParameterOverrideRecord transitParameterOverride,
      @Nullable Type genericInRetType, // NOT USED
      List<InternalTransition> stepRefPrms, // NOT USED
      List<Pair<String, String>> flowFactories, // NOT USED
      List<Pair<String, String>> flowRepos // NOT USED
      ) {
    final String parameterName = baseParameter.name;

    if (transitParameterOverride != null) {
      throw new IllegalStateException(
          String.format(
              "Function parameter of type [%s] can't be overridden by transit parameter. Flow: [%s] Function/Call: [%s] Parameter: [%s]",
              ParameterType.EVENT_BUS,
              flowTypeRecord.flowTypeName,
              functionOrCallName,
              parameterName));
    }

    validateFixedTypeParameter(
        baseParameter,
        parameterOverrideFromCall,
        null,
        FlowEventBus.class,
        flowTypeRecord.flowTypeName,
        functionOrCallName);

    return new ParameterCreationResult(
        new FunctionCallParameter(
            null,
            null,
            parameterName,
            ParameterType.EVENT_BUS,
            FlowEventBus.class,
            null,
            baseParameter.nullableAnnotation != null,
            false),
        ImmutableList.of());
  }
}
