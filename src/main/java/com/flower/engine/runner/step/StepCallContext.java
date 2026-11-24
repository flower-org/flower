package com.flower.engine.runner.step;

import com.flower.conf.FlowId;
import com.flower.conf.StepInfoPrm;
import com.flower.engine.runner.event.EventContext;
import com.flower.engine.runner.event.EventParametersProvider;
import com.flower.engine.runner.event.EventRunner;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public interface StepCallContext {
  StepInfoPrm getStepInfo();

  List<InternalTransition> getTransitions();

  List<Pair<String, String>> getFlowFactories();

  List<Pair<String, String>> getFlowRepos();

  CompletableFuture<InternalTransition> call(
      FlowId flowId,
      Object flowState,
      EventRunner eventRunner,
      EventContext eventContext,
      EventParametersProvider eventParametersProvider);

  StepParameterInitProfile stepParameterInitProfile();
}
