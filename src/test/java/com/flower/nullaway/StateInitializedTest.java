package com.flower.nullaway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.flower.anno.event.Concurrency;
import com.flower.anno.event.EventFunction;
import com.flower.anno.event.EventProfileContainer;
import com.flower.anno.event.EventProfiles;
import com.flower.anno.event.EventType;
import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.State;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.params.common.In;
import com.flower.anno.params.common.InOut;
import com.flower.anno.params.common.Out;
import com.flower.anno.params.transit.Terminal;
import com.flower.conf.InOutPrm;
import com.flower.conf.OutPrm;
import com.flower.conf.Transition;
import com.flower.engine.Flower;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;

public class StateInitializedTest {
  static Flower flower(Class<?> flowType, Class<?>... eventProfiles) {
    Flower flower = new Flower();
    flower.registerFlow(flowType);
    for (Class<?> eventProfile : eventProfiles) {
      flower.registerEventProfile(eventProfile);
    }
    flower.initialize();
    return flower;
  }

  static <T> T run(Flower flower, Class<T> flowType, T flow) throws Exception {
    return flower.getFlowExec(flowType).runFlow(flow).getFuture().get(10, TimeUnit.SECONDS);
  }

  // ---------------- Flows ----------------

  @Test
  void flow_declaredInitialized_treatedAsInitialized() throws Exception {
    Flower flower = flower(SI_InitializedFlow.class);
    try {
      assertEquals(1, run(flower, SI_InitializedFlow.class, new SI_InitializedFlow()).result);
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void flow_initializerWithoutDeclaration_notTreatedAsInitialized() {
    Flower flower = new Flower();
    try {
      flower.registerFlow(SI_UndeclaredFlow.class);
      IllegalStateException e = assertThrows(IllegalStateException.class, flower::initialize);
      assertTrue(e.getMessage().contains("expects to find the state field [count] initialized"), e.getMessage());
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void flow_primitive_treatedAsInitialized() throws Exception {
    Flower flower = flower(SI_PrimitiveFlow.class);
    try {
      assertEquals(1, run(flower, SI_PrimitiveFlow.class, new SI_PrimitiveFlow()).result);
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void flow_declaredInitializedButNull_flowFails() {
    Flower flower = flower(SI_InitializedFlow.class);
    try {
      SI_InitializedFlow flow = new SI_InitializedFlow();
      flow.count = null;
      ExecutionException e =
          assertThrows(ExecutionException.class, () -> run(flower, SI_InitializedFlow.class, flow));
      assertTrue(
          e.getCause().getMessage().contains(
              "State field [count] of Flow [com.flower.nullaway.SI_InitializedFlow] is declared @State(initialized = true), but it's null"),
          e.getCause().getMessage());
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void nullableAndDeclaredInitialized_rejected() {
    Flower flower = new Flower();
    try {
      flower.registerFlow(SI_NullableInitializedFlow.class);
      IllegalStateException e = assertThrows(IllegalStateException.class, flower::initialize);
      assertTrue(
          e.getMessage().contains("can't be both @Nullable and @State(initialized = true)"),
          e.getMessage());
    } finally {
      flower.shutdownScheduler();
    }
  }

  // ---------------- EventProfiles ----------------

  @Test
  void eventProfile_declaredInitialized_treatedAsInitialized() throws Exception {
    SI_CountingProfile.seen.set(null);
    Flower flower = flower(SI_EventFlow.class, SI_CountingProfile.class);
    try {
      run(flower, SI_EventFlow.class, new SI_EventFlow());
      assertEquals(1, SI_CountingProfile.seen.get());
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void eventProfile_primitive_treatedAsInitialized() throws Exception {
    SI_PrimitiveProfile.seen.set(null);
    Flower flower = flower(SI_PrimitiveEventFlow.class, SI_PrimitiveProfile.class);
    try {
      run(flower, SI_PrimitiveEventFlow.class, new SI_PrimitiveEventFlow());
      assertEquals(1, SI_PrimitiveProfile.seen.get());
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void eventProfile_declaredInitializedButNull_flowFails() {
    Flower flower = flower(SI_NullProfileFlow.class, SI_NullProfile.class);
    try {
      ExecutionException e =
          assertThrows(
              ExecutionException.class,
              () -> run(flower, SI_NullProfileFlow.class, new SI_NullProfileFlow()));
      assertTrue(
          e.getCause().getMessage().contains(
              "State field [count] of EventProfile [SI_NullProfile] is declared @State(initialized = true), but it's null"),
          e.getCause().getMessage());
    } finally {
      flower.shutdownScheduler();
    }
  }
}

// ---------------- Flows ----------------

@FlowType(firstStep = "step")
class SI_InitializedFlow {
  @State(initialized = true) Integer count = 0;
  @State Integer result;

  @SimpleStepFunction
  static Transition step(@In Integer count, @Out OutPrm<Integer> result, @Terminal Transition end) {
    result.setOutValue(count + 1);
    return end;
  }
}

@FlowType(firstStep = "step")
class SI_UndeclaredFlow {
  @State Integer count = 0;

  @SimpleStepFunction
  static Transition step(@In Integer count, @Terminal Transition end) {
    return end;
  }
}

@FlowType(firstStep = "step")
class SI_PrimitiveFlow {
  @State int count;
  @State Integer result;

  @SimpleStepFunction
  static Transition step(@In int count, @Out OutPrm<Integer> result, @Terminal Transition end) {
    result.setOutValue(count + 1);
    return end;
  }
}

@FlowType(firstStep = "step")
class SI_NullableInitializedFlow {
  @Nullable @State(initialized = true) Integer count = 0;

  @SimpleStepFunction
  static Transition step(@Terminal Transition end) {
    return end;
  }
}

// ---------------- EventProfiles ----------------

@FlowType(firstStep = "step")
@EventProfiles({SI_CountingProfile.class})
class SI_EventFlow {
  @SimpleStepFunction
  static Transition step(@Terminal Transition end) {
    return end;
  }
}

@EventProfileContainer(name = "SI_CountingProfile")
class SI_CountingProfile {
  static final AtomicReference<Integer> seen = new AtomicReference<>();

  @State(initialized = true) Integer stepCount = 0;

  public SI_CountingProfile() {}

  @EventFunction(types = EventType.BEFORE_STEP, concurrency = Concurrency.SYNCHRONIZED)
  static void countStep(@InOut InOutPrm<Integer> stepCount) {
    stepCount.setOutValue(stepCount.getInValue() + 1);
  }

  @EventFunction(types = EventType.AFTER_FLOW, concurrency = Concurrency.SYNCHRONIZED)
  static void report(@In Integer stepCount) {
    seen.set(stepCount);
  }
}

@FlowType(firstStep = "step")
@EventProfiles({SI_PrimitiveProfile.class})
class SI_PrimitiveEventFlow {
  @SimpleStepFunction
  static Transition step(@Terminal Transition end) {
    return end;
  }
}

@EventProfileContainer(name = "SI_PrimitiveProfile")
class SI_PrimitiveProfile {
  static final AtomicReference<Integer> seen = new AtomicReference<>();

  @State int stepCount;

  public SI_PrimitiveProfile() {}

  @EventFunction(types = EventType.BEFORE_STEP, concurrency = Concurrency.SYNCHRONIZED)
  static void countStep(@InOut InOutPrm<Integer> stepCount) {
    stepCount.setOutValue(stepCount.getInValue() + 1);
  }

  @EventFunction(types = EventType.AFTER_FLOW, concurrency = Concurrency.SYNCHRONIZED)
  static void report(@In Integer stepCount) {
    seen.set(stepCount);
  }
}

@FlowType(firstStep = "step")
@EventProfiles({SI_NullProfile.class})
class SI_NullProfileFlow {
  @SimpleStepFunction
  static Transition step(@Terminal Transition end) {
    return end;
  }
}

@EventProfileContainer(name = "SI_NullProfile")
class SI_NullProfile {
  @State(initialized = true) Integer count;

  public SI_NullProfile() {}
}
