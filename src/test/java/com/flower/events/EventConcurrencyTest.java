package com.flower.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.flower.anno.event.Concurrency;
import com.flower.anno.event.EventCall;
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
import com.flower.conf.NullableInOutPrm;
import com.flower.conf.OutPrm;
import com.flower.conf.Transition;
import com.flower.engine.Flower;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class EventConcurrencyTest {
  static Flower flower(Class<?> flowType, Class<?>... eventProfiles) {
    Flower flower = new Flower(4);
    flower.registerFlow(flowType);
    for (Class<?> eventProfile : eventProfiles) {
      flower.registerEventProfile(eventProfile);
    }
    flower.initialize();
    return flower;
  }

  static <T> void runFlow(Flower flower, Class<T> flowType, T flow) throws Exception {
    flower.getFlowExec(flowType).runFlow(flow).getFuture().get(10, TimeUnit.SECONDS);
  }

  @Test
  void defaultConcurrencyIsNonBlocking() throws NoSuchMethodException {
    assertEquals(
        Concurrency.NON_BLOCKING, EventFunction.class.getMethod("concurrency").getDefaultValue());
    assertEquals(
        Concurrency.NON_BLOCKING, EventCall.class.getMethod("concurrency").getDefaultValue());
  }

  @Test
  void stagesRunInOrder() throws Exception {
    EC_OrderProfile.reset();
    Flower flower = flower(EC_OrderFlow.class, EC_OrderProfile.class);
    try {
      runFlow(flower, EC_OrderFlow.class, new EC_OrderFlow());

      // NON_BLOCKING was started before BLOCKING finished (BLOCKING waits for it)
      assertTrue(EC_OrderProfile.blockingSawNonBlockingStarted.get());
      // BLOCKING -> SYNCHRONIZED -> FINALIZER -> Flow
      List<String> calls =
          EC_OrderProfile.calls.stream()
              .filter(c -> !c.equals("nonBlocking"))
              .collect(Collectors.toList());
      assertEquals(
          List.of(
              "blocking", "blocking", "synchronized", "synchronized", "finalizer", "finalizer",
              "step"),
          calls);
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void profilesRunSynchronizedInParallel_finalizerWaitsForAllProfiles() throws Exception {
    EC_ProfileA.reset();
    Flower flower = flower(EC_TwoProfilesFlow.class, EC_ProfileA.class, EC_ProfileB.class);
    try {
      runFlow(flower, EC_TwoProfilesFlow.class, new EC_TwoProfilesFlow());

      // SYNCHRONIZED of each profile waits for the other's: only possible if profiles run in
      // parallel
      assertTrue(EC_ProfileA.synchronizedRanConcurrently.get());
      // FINALIZER ran after SYNCHRONIZED of both profiles finished
      assertTrue(EC_ProfileA.finalizerSawAllSynchronizedDone.get());
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void blockingFunctionsRunConcurrentlyAndBlockTheFlow() throws Exception {
    EC_BlockingProfile.reset();
    Flower flower = flower(EC_BlockingFlow.class, EC_BlockingProfile.class);
    try {
      runFlow(flower, EC_BlockingFlow.class, new EC_BlockingFlow());

      // Each BLOCKING function waits for the other one: only possible if they run concurrently
      assertTrue(EC_BlockingProfile.bothRanConcurrently.get());
      assertTrue(EC_BlockingFlow.stepSawBlockingDone.get());
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void nonBlockingFunctionsDontBlockTheFlow() throws Exception {
    EC_NonBlockingProfile.reset();
    Flower flower = flower(EC_NonBlockingFlow.class, EC_NonBlockingProfile.class);
    try {
      // The NON_BLOCKING function waits until the step has run: only possible if it doesn't block it
      runFlow(flower, EC_NonBlockingFlow.class, new EC_NonBlockingFlow());
      assertTrue(EC_NonBlockingProfile.done.await(10, TimeUnit.SECONDS));
      assertTrue(EC_NonBlockingProfile.sawStep.get());
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void synchronizedExceptionIsSwallowed() throws Exception {
    EC_SyncThrowsProfile.reset();
    Flower flower = flower(EC_SyncThrowsFlow.class, EC_SyncThrowsProfile.class);
    try {
      runFlow(flower, EC_SyncThrowsFlow.class, new EC_SyncThrowsFlow());
      assertEquals(List.of("throws", "finalizer", "step"), EC_SyncThrowsProfile.calls);
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void nonBlockingWithOut_rejected() {
    assertStateUpdateRejected(EC_NonBlockingOutFlow.class, EC_NonBlockingOutProfile.class, "NON_BLOCKING");
  }

  @Test
  void blockingWithInOut_rejected() {
    assertStateUpdateRejected(
        EC_BlockingInOutFlow.class, EC_BlockingInOutProfile.class, "BLOCKING");
  }

  @Test
  void synchronizedWithOut_allowed() throws Exception {
    EC_SyncOutProfile.seen.set(null);
    Flower flower = flower(EC_SyncOutFlow.class, EC_SyncOutProfile.class);
    try {
      runFlow(flower, EC_SyncOutFlow.class, new EC_SyncOutFlow());
      assertEquals(42, EC_SyncOutProfile.seen.get());
    } finally {
      flower.shutdownScheduler();
    }
  }

  static void assertStateUpdateRejected(Class<?> flowType, Class<?> profile, String concurrency) {
    Flower flower = new Flower();
    try {
      flower.registerFlow(flowType);
      flower.registerEventProfile(profile);
      IllegalStateException e = assertThrows(IllegalStateException.class, flower::initialize);
      assertTrue(
          e.getMessage()
              .contains("Event Function with concurrency [" + concurrency + "] can't update state"),
          e.getMessage());
    } finally {
      flower.shutdownScheduler();
    }
  }
}

// ---------------- Order ----------------

@FlowType(name = "Test", firstStep = "step")
@EventProfiles({EC_OrderProfile.class})
class EC_OrderFlow {
  @SimpleStepFunction
  static Transition step(@Terminal Transition end) {
    EC_OrderProfile.calls.add("step");
    return end;
  }
}

@EventProfileContainer(name = "EC_OrderProfile")
class EC_OrderProfile {
  static List<String> calls = new CopyOnWriteArrayList<>();
  static CountDownLatch nonBlockingStarted = new CountDownLatch(1);
  static final AtomicBoolean blockingSawNonBlockingStarted = new AtomicBoolean();

  public EC_OrderProfile() {}

  static void reset() {
    calls = new CopyOnWriteArrayList<>();
    nonBlockingStarted = new CountDownLatch(1);
    blockingSawNonBlockingStarted.set(true);
  }

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.FINALIZER)
  static void finalizer1() {
    calls.add("finalizer");
  }

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.FINALIZER)
  static void finalizer2() {
    calls.add("finalizer");
  }

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.SYNCHRONIZED)
  static void synchronized1() {
    calls.add("synchronized");
  }

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.SYNCHRONIZED)
  static void synchronized2() {
    calls.add("synchronized");
  }

  static void blocking() throws InterruptedException {
    if (!nonBlockingStarted.await(5, TimeUnit.SECONDS)) {
      blockingSawNonBlockingStarted.set(false);
    }
    Thread.sleep(50);
    calls.add("blocking");
  }

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.BLOCKING)
  static void blocking1() throws InterruptedException {
    blocking();
  }

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.BLOCKING)
  static void blocking2() throws InterruptedException {
    blocking();
  }

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.NON_BLOCKING)
  static void nonBlocking() {
    nonBlockingStarted.countDown();
    calls.add("nonBlocking");
  }
}

// ---------------- Two profiles ----------------

@FlowType(name = "Test", firstStep = "step")
@EventProfiles({EC_ProfileA.class, EC_ProfileB.class})
class EC_TwoProfilesFlow {
  @SimpleStepFunction
  static Transition step(@Terminal Transition end) {
    return end;
  }
}

@EventProfileContainer(name = "EC_ProfileA")
class EC_ProfileA {
  static CountDownLatch synchronizedStarted = new CountDownLatch(2);
  static CountDownLatch synchronizedDone = new CountDownLatch(2);
  static final AtomicBoolean synchronizedRanConcurrently = new AtomicBoolean();
  static final AtomicBoolean finalizerSawAllSynchronizedDone = new AtomicBoolean();

  public EC_ProfileA() {}

  static void reset() {
    synchronizedStarted = new CountDownLatch(2);
    synchronizedDone = new CountDownLatch(2);
    synchronizedRanConcurrently.set(true);
    finalizerSawAllSynchronizedDone.set(false);
  }

  static void rendezvous() throws InterruptedException {
    synchronizedStarted.countDown();
    if (!synchronizedStarted.await(5, TimeUnit.SECONDS)) {
      synchronizedRanConcurrently.set(false);
    }
    Thread.sleep(50);
    synchronizedDone.countDown();
  }

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.SYNCHRONIZED)
  static void synchronizedA() throws InterruptedException {
    rendezvous();
  }

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.FINALIZER)
  static void finalizer() {
    finalizerSawAllSynchronizedDone.set(synchronizedDone.getCount() == 0);
  }
}

@EventProfileContainer(name = "EC_ProfileB")
class EC_ProfileB {
  public EC_ProfileB() {}

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.SYNCHRONIZED)
  static void synchronizedB() throws InterruptedException {
    EC_ProfileA.rendezvous();
  }
}

// ---------------- BLOCKING ----------------

@FlowType(name = "Test", firstStep = "step")
@EventProfiles({EC_BlockingProfile.class})
class EC_BlockingFlow {
  static final AtomicBoolean stepSawBlockingDone = new AtomicBoolean();

  @SimpleStepFunction
  static Transition step(@Terminal Transition end) {
    stepSawBlockingDone.set(EC_BlockingProfile.finished.getCount() == 0);
    return end;
  }
}

@EventProfileContainer(name = "EC_BlockingProfile")
class EC_BlockingProfile {
  static CountDownLatch started = new CountDownLatch(2);
  static CountDownLatch finished = new CountDownLatch(2);
  static final AtomicBoolean bothRanConcurrently = new AtomicBoolean();

  public EC_BlockingProfile() {}

  static void reset() {
    started = new CountDownLatch(2);
    finished = new CountDownLatch(2);
    bothRanConcurrently.set(true);
    EC_BlockingFlow.stepSawBlockingDone.set(false);
  }

  static void rendezvous() throws InterruptedException {
    started.countDown();
    if (!started.await(5, TimeUnit.SECONDS)) {
      bothRanConcurrently.set(false);
    }
    Thread.sleep(50);
    finished.countDown();
  }

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.BLOCKING)
  static void blocking1() throws InterruptedException {
    rendezvous();
  }

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.BLOCKING)
  static void blocking2() throws InterruptedException {
    rendezvous();
  }
}

// ---------------- NON_BLOCKING ----------------

@FlowType(name = "Test", firstStep = "step")
@EventProfiles({EC_NonBlockingProfile.class})
class EC_NonBlockingFlow {
  @SimpleStepFunction
  static Transition step(@Terminal Transition end) {
    EC_NonBlockingProfile.stepRan.countDown();
    return end;
  }
}

@EventProfileContainer(name = "EC_NonBlockingProfile")
class EC_NonBlockingProfile {
  static CountDownLatch stepRan = new CountDownLatch(1);
  static CountDownLatch done = new CountDownLatch(1);
  static final AtomicBoolean sawStep = new AtomicBoolean();

  public EC_NonBlockingProfile() {}

  static void reset() {
    stepRan = new CountDownLatch(1);
    done = new CountDownLatch(1);
    sawStep.set(false);
  }

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.NON_BLOCKING)
  static void nonBlocking() throws InterruptedException {
    sawStep.set(stepRan.await(5, TimeUnit.SECONDS));
    done.countDown();
  }
}

// ---------------- SYNCHRONIZED exception ----------------

@FlowType(name = "Test", firstStep = "step")
@EventProfiles({EC_SyncThrowsProfile.class})
class EC_SyncThrowsFlow {
  @SimpleStepFunction
  static Transition step(@Terminal Transition end) {
    EC_SyncThrowsProfile.calls.add("step");
    return end;
  }
}

@EventProfileContainer(name = "EC_SyncThrowsProfile")
class EC_SyncThrowsProfile {
  static List<String> calls = new CopyOnWriteArrayList<>();

  public EC_SyncThrowsProfile() {}

  static void reset() {
    calls = new CopyOnWriteArrayList<>();
  }

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.SYNCHRONIZED)
  static void throwing() {
    calls.add("throws");
    throw new RuntimeException("TestException");
  }

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.FINALIZER)
  static void finalizer() {
    calls.add("finalizer");
  }
}

// ---------------- Validation ----------------

@FlowType(name = "Test", firstStep = "step")
@EventProfiles({EC_NonBlockingOutProfile.class})
class EC_NonBlockingOutFlow {
  @SimpleStepFunction
  static Transition step(@Terminal Transition end) {
    return end;
  }
}

@EventProfileContainer(name = "EC_NonBlockingOutProfile")
class EC_NonBlockingOutProfile {
  @State Integer value;

  public EC_NonBlockingOutProfile() {}

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.NON_BLOCKING)
  static void event(@Out OutPrm<Integer> value) {
    value.setOutValue(1);
  }
}

@FlowType(name = "Test", firstStep = "step")
@EventProfiles({EC_BlockingInOutProfile.class})
class EC_BlockingInOutFlow {
  @SimpleStepFunction
  static Transition step(@Terminal Transition end) {
    return end;
  }
}

@EventProfileContainer(name = "EC_BlockingInOutProfile")
class EC_BlockingInOutProfile {
  @State Integer value;

  public EC_BlockingInOutProfile() {}

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.BLOCKING)
  static void event(@InOut NullableInOutPrm<Integer> value) {
    value.setOutValue(1);
  }
}

@FlowType(name = "Test", firstStep = "step")
@EventProfiles({EC_SyncOutProfile.class})
class EC_SyncOutFlow {
  @SimpleStepFunction
  static Transition step(@Terminal Transition end) {
    return end;
  }
}

@EventProfileContainer(name = "EC_SyncOutProfile")
class EC_SyncOutProfile {
  static final AtomicReference<Integer> seen = new AtomicReference<>();

  @State Integer value;

  public EC_SyncOutProfile() {}

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.SYNCHRONIZED)
  static void write(@Out OutPrm<Integer> value) {
    value.setOutValue(42);
  }

  @EventFunction(types = EventType.BEFORE_STEP)
  static void read(@In Integer value) {
    seen.set(value);
  }
}
