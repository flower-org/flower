package com.flower.parameters;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.flower.anno.event.EventCall;
import com.flower.anno.event.EventFunction;
import com.flower.anno.event.EventProfileContainer;
import com.flower.anno.event.EventProfiles;
import com.flower.anno.event.EventType;
import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.GlobalFunctionContainer;
import com.flower.anno.flow.State;
import com.flower.anno.functions.GlobalFunction;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.functions.StepCall;
import com.flower.anno.functions.StepFunction;
import com.flower.anno.functions.TransitFunction;
import com.flower.anno.params.common.EventBus;
import com.flower.anno.params.common.In;
import com.flower.anno.params.common.Out;
import com.flower.anno.params.transit.StepRef;
import com.flower.anno.params.transit.Terminal;
import com.flower.conf.OutPrm;
import com.flower.conf.Transition;
import com.flower.engine.Flower;
import com.flower.eventbus.FlowEventBus;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class EventBusParameterTest {
  static <T> T run(Flower flower, Class<T> flowType, T flow) throws Exception {
    return flower.getFlowExec(flowType).runFlow(flow).getFuture().get(10, TimeUnit.SECONDS);
  }

  @Test
  void sameBusAcrossStepsAndTransits_andUsableAcrossSteps() throws Exception {
    Flower flower = new Flower();
    flower.registerFlow(EB_Flow.class);
    flower.initialize();
    try {
      EB_Flow result = run(flower, EB_Flow.class, new EB_Flow());

      assertNotNull(result.stepBus);
      assertTrue(result.transitSame);
      assertTrue(result.nextStepSame);
      // The future registered in step1 was delivered by next() in step2
      assertTrue(result.delivered);
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void eachFlowInstanceHasItsOwnBus() throws Exception {
    Flower flower = new Flower();
    flower.registerFlow(EB_Flow.class);
    flower.initialize();
    try {
      EB_Flow result1 = run(flower, EB_Flow.class, new EB_Flow());
      EB_Flow result2 = run(flower, EB_Flow.class, new EB_Flow());

      assertNotSame(result1.stepBus, result2.stepBus);
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void stepCallToGlobalFunctionGetsTheFlowsBus() throws Exception {
    Flower flower = new Flower();
    flower.registerGlobalFunctions(EB_Globals.class);
    flower.registerFlow(EB_CallFlow.class);
    flower.initialize();
    try {
      EB_CallFlow result = run(flower, EB_CallFlow.class, new EB_CallFlow());

      assertNotNull(result.stepBus);
      assertTrue(result.transitSame);
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void rejectedInEventFunction() {
    Flower flower = new Flower();
    try {
      Throwable e =
          assertThrows(
              Throwable.class,
              () -> {
                flower.registerFlow(EB_EventFlow.class);
                flower.registerEventProfile(EB_EventProfile.class);
                flower.initialize();
              });
      assertTrue(
          e.getMessage().contains("annotated as @EventBus can only be used in StepFunction"),
          e.getMessage());
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void rejectedInGlobalFunctionCalledFromEvent() {
    Flower flower = new Flower();
    try {
      flower.registerGlobalFunctions(EB_EventGlobals.class);
      flower.registerFlow(EB_EventCallFlow.class);
      flower.registerEventProfile(EB_EventCallProfile.class);
      // An @EventCall must declare all GlobalFunction parameters, so @EventBus is rejected on it
      Throwable e = assertThrows(Throwable.class, flower::initialize);
      assertTrue(
          e.getMessage().contains("annotated as @EventBus can only be used in StepFunction"),
          e.getMessage());
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void wrongTypeRejected() {
    Flower flower = new Flower();
    try {
      Throwable e =
          assertThrows(
              Throwable.class,
              () -> {
                flower.registerFlow(EB_WrongTypeFlow.class);
                flower.initialize();
              });
      assertTrue(
          e.getMessage()
              .contains(
                  "annotated as @EventBus should be of type "
                      + FlowEventBus.class.getCanonicalName()),
          e.getMessage());
    } finally {
      flower.shutdownScheduler();
    }
  }
}

@FlowType(name = "Test", firstStep = "step1")
class EB_Flow {
  @State FlowEventBus stepBus;
  @State CompletableFuture<String> registered;
  @State Boolean transitSame;
  @State Boolean nextStepSame;
  @State Boolean delivered;

  @StepFunction(transit = "transit1")
  static void step1(
      @EventBus FlowEventBus bus,
      @Out OutPrm<FlowEventBus> stepBus,
      @Out OutPrm<CompletableFuture<String>> registered) {
    stepBus.setOutValue(bus);
    CompletableFuture<String> future = new CompletableFuture<>();
    bus.register(future);
    registered.setOutValue(future);
  }

  @TransitFunction
  static Transition transit1(
      @EventBus FlowEventBus bus,
      @In FlowEventBus stepBus,
      @Out OutPrm<Boolean> transitSame,
      @StepRef Transition step2) {
    transitSame.setOutValue(bus == stepBus);
    return step2;
  }

  @SimpleStepFunction
  static CompletableFuture<Transition> step2(
      @EventBus FlowEventBus bus,
      @In FlowEventBus stepBus,
      @In CompletableFuture<String> registered,
      @Out OutPrm<Boolean> nextStepSame,
      @Out OutPrm<Boolean> delivered,
      @Terminal Transition end) {
    nextStepSame.setOutValue(bus == stepBus);
    registered.complete("fired");
    return bus.next()
        .thenApply(
            fired -> {
              delivered.setOutValue(fired == registered);
              return end;
            });
  }
}

@GlobalFunctionContainer
class EB_Globals {
  @GlobalFunction
  static void capture(@EventBus FlowEventBus bus, @Out OutPrm<FlowEventBus> stepBus) {
    stepBus.setOutValue(bus);
  }
}

@FlowType(name = "Test", firstStep = "step")
class EB_CallFlow {
  @State FlowEventBus stepBus;
  @State Boolean transitSame;

  @StepCall(globalFunctionContainer = EB_Globals.class, globalFunctionName = "capture", transit = "transit")
  static void step(@EventBus FlowEventBus bus, @Out OutPrm<FlowEventBus> stepBus) {}

  @TransitFunction
  static Transition transit(
      @EventBus FlowEventBus bus,
      @In FlowEventBus stepBus,
      @Out OutPrm<Boolean> transitSame,
      @Terminal Transition end) {
    transitSame.setOutValue(bus == stepBus);
    return end;
  }
}

@FlowType(name = "Test", firstStep = "step")
@EventProfiles({EB_EventProfile.class})
class EB_EventFlow {
  @SimpleStepFunction
  static Transition step(@Terminal Transition end) {
    return end;
  }
}

@EventProfileContainer(name = "EB_EventProfile")
class EB_EventProfile {
  public EB_EventProfile() {}

  @EventFunction(types = EventType.BEFORE_FLOW)
  static void event(@EventBus FlowEventBus bus) {}
}

@GlobalFunctionContainer
class EB_EventGlobals {
  @GlobalFunction
  static void event(@EventBus FlowEventBus bus) {}
}

@FlowType(name = "Test", firstStep = "step")
@EventProfiles({EB_EventCallProfile.class})
class EB_EventCallFlow {
  @SimpleStepFunction
  static Transition step(@Terminal Transition end) {
    return end;
  }
}

@EventProfileContainer(name = "EB_EventCallProfile")
class EB_EventCallProfile {
  public EB_EventCallProfile() {}

  @EventCall(
      globalFunctionContainer = EB_EventGlobals.class,
      globalFunctionName = "event",
      types = EventType.BEFORE_FLOW)
  static void event(@EventBus FlowEventBus bus) {}
}

@FlowType(name = "Test", firstStep = "step")
class EB_WrongTypeFlow {
  @SimpleStepFunction
  static Transition step(@EventBus Object bus, @Terminal Transition end) {
    return end;
  }
}
