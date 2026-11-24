package com.flower.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.flower.anno.event.Concurrency;
import com.flower.anno.event.EventFunction;
import com.flower.anno.event.EventProfileContainer;
import com.flower.anno.event.EventProfiles;
import com.flower.anno.event.EventType;
import com.flower.anno.flow.FlowType;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.params.transit.Terminal;
import com.flower.conf.EventExceptionHandler;
import com.flower.conf.EventFunctionFailure;
import com.flower.conf.Transition;
import com.flower.engine.Flower;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class EventExceptionHandlerTest {
  static Flower flower(Class<?> flowType, Class<?> eventProfile) {
    Flower flower = new Flower(4);
    flower.registerFlow(flowType);
    flower.registerEventProfile(eventProfile);
    flower.initialize();
    return flower;
  }

  static <T> void run(Flower flower, Class<T> flowType, T flow) throws Exception {
    flower.getFlowExec(flowType).runFlow(flow).getFuture().get(10, TimeUnit.SECONDS);
  }

  /** Captures records of the "com.flower.events" logger. */
  static class CapturingLogHandler extends Handler {
    final List<LogRecord> records = new CopyOnWriteArrayList<>();
    final CountDownLatch received;

    CapturingLogHandler(int expected) {
      received = new CountDownLatch(expected);
    }

    @Override
    public void publish(LogRecord record) {
      records.add(record);
      received.countDown();
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}
  }

  @Test
  void customHandler_receivesNonFinalizerFailuresWithContext() throws Exception {
    List<EventFunctionFailure> failures = new CopyOnWriteArrayList<>();
    CountDownLatch allReported = new CountDownLatch(3);
    Flower flower = flower(EH_Flow.class, EH_FailingProfile.class);
    try {
      flower.setEventExceptionHandler(
          failure -> {
            failures.add(failure);
            allReported.countDown();
          });
      run(flower, EH_Flow.class, new EH_Flow());
      // NON_BLOCKING isn't awaited by the Flow
      assertTrue(allReported.await(10, TimeUnit.SECONDS));

      Map<String, EventFunctionFailure> byFunction =
          failures.stream()
              .collect(Collectors.toMap(EventFunctionFailure::eventFunctionName, Function.identity()));
      assertEquals(3, byFunction.size());
      for (Concurrency concurrency :
          List.of(Concurrency.NON_BLOCKING, Concurrency.BLOCKING, Concurrency.SYNCHRONIZED)) {
        EventFunctionFailure failure = byFunction.get(concurrency.name().toLowerCase());
        assertEquals(concurrency, failure.concurrency());
        assertEquals(EventType.BEFORE_FLOW, failure.eventType());
        assertEquals("EH_FailingProfile", failure.eventProfileName());
        assertEquals("Test", failure.flowInfo().flowName());
        assertEquals(concurrency.name(), failure.exception().getMessage());
      }
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void finalizerFailure_failsFlow_notReported() {
    List<EventFunctionFailure> failures = new CopyOnWriteArrayList<>();
    Flower flower = flower(EH_FinalizerFlow.class, EH_FinalizerProfile.class);
    try {
      flower.setEventExceptionHandler(failures::add);
      ExecutionException e =
          assertThrows(
              ExecutionException.class, () -> run(flower, EH_FinalizerFlow.class, new EH_FinalizerFlow()));
      assertEquals("FINALIZER", e.getCause().getMessage());
      assertTrue(failures.isEmpty(), failures.toString());
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void throwingHandler_doesntAffectFlow_andIsLogged() throws Exception {
    EH_SyncProfile.secondRan.set(false);
    CapturingLogHandler log = new CapturingLogHandler(1);
    EventExceptionHandler.LOGGER.addHandler(log);
    Flower flower = flower(EH_SyncFlow.class, EH_SyncProfile.class);
    try {
      flower.setEventExceptionHandler(
          failure -> {
            throw new IllegalStateException("HANDLER_FAILED");
          });
      run(flower, EH_SyncFlow.class, new EH_SyncFlow());
      // The SYNCHRONIZED function after the failing one still ran
      assertTrue(EH_SyncProfile.secondRan.get());

      assertTrue(log.received.await(10, TimeUnit.SECONDS));
      LogRecord record = log.records.get(0);
      assertEquals(Level.WARNING, record.getLevel());
      assertTrue(record.getMessage().contains("EventExceptionHandler failed"), record.getMessage());
      assertEquals("HANDLER_FAILED", record.getThrown().getMessage());
      assertEquals("FIRST_FAILED", record.getThrown().getSuppressed()[0].getMessage());
    } finally {
      EventExceptionHandler.LOGGER.removeHandler(log);
      flower.shutdownScheduler();
    }
  }

  @Test
  void defaultHandler_logsWarning() throws Exception {
    EH_SyncProfile.secondRan.set(false);
    CapturingLogHandler log = new CapturingLogHandler(1);
    EventExceptionHandler.LOGGER.addHandler(log);
    Flower flower = flower(EH_SyncFlow.class, EH_SyncProfile.class);
    try {
      run(flower, EH_SyncFlow.class, new EH_SyncFlow());

      assertTrue(log.received.await(10, TimeUnit.SECONDS));
      LogRecord record = log.records.get(0);
      assertEquals(Level.WARNING, record.getLevel());
      assertTrue(
          record.getMessage().contains("Event Function [EH_SyncProfile.a_first] (SYNCHRONIZED) failed on BEFORE_FLOW"),
          record.getMessage());
      assertEquals("FIRST_FAILED", record.getThrown().getMessage());
    } finally {
      EventExceptionHandler.LOGGER.removeHandler(log);
      flower.shutdownScheduler();
    }
  }
}

@FlowType(name = "Test", firstStep = "step")
@EventProfiles({EH_FailingProfile.class})
class EH_Flow {
  @SimpleStepFunction
  static Transition step(@Terminal Transition end) {
    return end;
  }
}

@EventProfileContainer(name = "EH_FailingProfile")
class EH_FailingProfile {
  public EH_FailingProfile() {}

  @EventFunction(name = "non_blocking", types = EventType.BEFORE_FLOW, concurrency = Concurrency.NON_BLOCKING)
  static void nonBlocking() {
    throw new RuntimeException("NON_BLOCKING");
  }

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.BLOCKING)
  static void blocking() {
    throw new RuntimeException("BLOCKING");
  }

  @EventFunction(name = "synchronized", types = EventType.BEFORE_FLOW, concurrency = Concurrency.SYNCHRONIZED)
  static void synchronizedFunction() {
    throw new RuntimeException("SYNCHRONIZED");
  }
}

@FlowType(name = "Test", firstStep = "step")
@EventProfiles({EH_FinalizerProfile.class})
class EH_FinalizerFlow {
  @SimpleStepFunction
  static Transition step(@Terminal Transition end) {
    return end;
  }
}

@EventProfileContainer(name = "EH_FinalizerProfile")
class EH_FinalizerProfile {
  public EH_FinalizerProfile() {}

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.FINALIZER)
  static void finalizer() {
    throw new RuntimeException("FINALIZER");
  }
}

@FlowType(name = "Test", firstStep = "step")
@EventProfiles({EH_SyncProfile.class})
class EH_SyncFlow {
  @SimpleStepFunction
  static Transition step(@Terminal Transition end) {
    return end;
  }
}

@EventProfileContainer(name = "EH_SyncProfile")
class EH_SyncProfile {
  static final AtomicBoolean secondRan = new AtomicBoolean();

  public EH_SyncProfile() {}

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.SYNCHRONIZED)
  static void a_first() {
    throw new RuntimeException("FIRST_FAILED");
  }

  // Runs after a_first if declaration order is preserved; otherwise it runs first. Either way it
  // must run despite a_first's failure.
  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.SYNCHRONIZED)
  static void b_second() {
    secondRan.set(true);
  }
}
