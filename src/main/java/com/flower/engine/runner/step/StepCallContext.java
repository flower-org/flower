package com.flower.engine.runner.step;

import com.flower.conf.FlowId;
import com.flower.conf.StepInfoPrm;
import com.flower.engine.function.FunctionCallParameter;
import com.flower.engine.function.ParameterType;
import com.flower.engine.runner.callfunc.FunctionCallContext;
import com.flower.engine.runner.event.EventContext;
import com.flower.engine.runner.event.EventParametersProvider;
import com.flower.engine.runner.event.EventRunner;
import com.flower.eventbus.FlowEventBus;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import java.util.concurrent.Executor;

public interface StepCallContext {
  StepInfoPrm getStepInfo();

  /** @return the Step's note for diagrams, empty if none */
  String getNote();

  /** @return State fields read by the Step's functions (@In, @InOut), in declaration order */
  Set<String> getStateReads();

  /** @return State fields written by the Step's functions (@Out, @InOut, returnTo), in declaration order */
  Set<String> getStateWrites();

  static Set<String> stateFields(List<FunctionCallContext> functionCalls, boolean writes) {
    Set<String> fields = new LinkedHashSet<>();
    for (FunctionCallContext functionCall : functionCalls) {
      for (FunctionCallParameter parameter : functionCall.functionParameters) {
        ParameterType type = parameter.getFunctionParameterType();
        boolean matches = type == ParameterType.IN_OUT || type == (writes ? ParameterType.OUT : ParameterType.IN);
        if (matches && parameter.getStateFieldName() != null) {
          fields.add(parameter.getStateFieldName());
        }
      }
    }
    return fields;
  }

  List<InternalTransition> getTransitions();

  List<Pair<String, String>> getFlowFactories();

  List<Pair<String, String>> getFlowRepos();

  CompletableFuture<InternalTransition> call(
      FlowId flowId,
      Object flowState,
      EventRunner eventRunner,
      EventContext eventContext,
      EventParametersProvider eventParametersProvider,
      @Nullable FlowEventBus eventBus,
      Executor functionExecutor);

  StepParameterInitProfile stepParameterInitProfile();

  /** @return whether any function of this step takes an @EventBus parameter */
  boolean usesEventBus();
}
