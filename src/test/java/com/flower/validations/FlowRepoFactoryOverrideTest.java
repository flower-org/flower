package com.flower.validations;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.GlobalFunctionContainer;
import com.flower.anno.functions.GlobalFunction;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.functions.StepCall;
import com.flower.anno.functions.TransitFunction;
import com.flower.anno.params.step.FlowFactory;
import com.flower.anno.params.step.FlowRepo;
import com.flower.anno.params.transit.Terminal;
import com.flower.conf.FlowFactoryPrm;
import com.flower.conf.FlowRepoPrm;
import com.flower.conf.Transition;
import com.flower.engine.Flower;
import org.junit.jupiter.api.Test;

/** A @StepCall can't override a GlobalFunction's @FlowRepo parameter with @FlowFactory, or vice versa. */
public class FlowRepoFactoryOverrideTest {
  static void assertIncompatible(Class<?> flowType) {
    Flower flower = new Flower();
    try {
      flower.registerGlobalFunctions(FRO_Globals.class);
      flower.registerFlow(FRO_ChildFlow.class);
      flower.registerFlow(flowType);
      Throwable e = assertThrows(Throwable.class, flower::initialize);
      assertTrue(
          String.valueOf(e.getMessage()).contains("Function override parameter Flower Type is incompatible"),
          e.toString());
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void flowRepoOverriddenWithFlowFactory_rejected() {
    assertIncompatible(FRO_RepoAsFactoryFlow.class);
  }

  @Test
  void flowFactoryOverriddenWithFlowRepo_rejected() {
    assertIncompatible(FRO_FactoryAsRepoFlow.class);
  }
}

@FlowType(firstStep = "step")
class FRO_ChildFlow {
  @SimpleStepFunction
  static Transition step(@Terminal Transition end) {
    return end;
  }
}

@GlobalFunctionContainer
class FRO_Globals {
  @GlobalFunction
  static void withRepo(@FlowRepo FlowRepoPrm<FRO_ChildFlow> children) {}

  @GlobalFunction
  static void withFactory(@FlowFactory FlowFactoryPrm<FRO_ChildFlow> children) {}
}

@FlowType(firstStep = "step")
class FRO_RepoAsFactoryFlow {
  @StepCall(globalFunctionContainer = FRO_Globals.class, globalFunctionName = "withRepo", transit = "transit")
  static void step(@FlowFactory FlowFactoryPrm<FRO_ChildFlow> children) {}

  @TransitFunction
  static Transition transit(@Terminal Transition end) {
    return end;
  }
}

@FlowType(firstStep = "step")
class FRO_FactoryAsRepoFlow {
  @StepCall(globalFunctionContainer = FRO_Globals.class, globalFunctionName = "withFactory", transit = "transit")
  static void step(@FlowRepo FlowRepoPrm<FRO_ChildFlow> children) {}

  @TransitFunction
  static Transition transit(@Terminal Transition end) {
    return end;
  }
}
