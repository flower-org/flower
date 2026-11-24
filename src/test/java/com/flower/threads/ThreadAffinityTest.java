package com.flower.threads;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.flower.anno.event.Concurrency;
import com.flower.anno.event.EventFunction;
import com.flower.anno.event.EventProfileContainer;
import com.flower.anno.event.EventProfiles;
import com.flower.anno.event.EventType;
import com.flower.anno.flow.FlowType;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.functions.StepFunction;
import com.flower.anno.functions.TransitFunction;
import com.flower.anno.params.transit.Terminal;
import com.flower.conf.Transition;
import com.flower.engine.Flower;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Step, Transit and Event Functions run on Flower's scheduler, even when the stage before them was
 * completed by another thread (e.g. an asynchronous client's thread, or another Flower engine's).
 */
public class ThreadAffinityTest {
  static ScheduledExecutorService namedScheduler(String prefix) {
    AtomicInteger counter = new AtomicInteger();
    return Executors.newScheduledThreadPool(
        2,
        r -> {
          Thread thread = new Thread(r, prefix + "-" + counter.incrementAndGet());
          thread.setDaemon(true);
          return thread;
        });
  }

  /** Returns a future completed from a new non-Flower thread, after a short delay. */
  static <T> CompletableFuture<T> completeExternally(T value) {
    CompletableFuture<T> future = new CompletableFuture<>();
    Thread thread =
        new Thread(
            () -> {
              try {
                Thread.sleep(50);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
              future.complete(value);
            },
            "external-client-thread");
    thread.setDaemon(true);
    thread.start();
    return future;
  }

  static Flower flower(
      ScheduledExecutorService scheduler, Class<?> flowType, Class<?>... eventProfiles) {
    Flower flower = new Flower(scheduler);
    flower.registerFlow(flowType);
    for (Class<?> eventProfile : eventProfiles) {
      flower.registerEventProfile(eventProfile);
    }
    flower.initialize();
    return flower;
  }

  static <T> void run(Flower flower, Class<T> flowType, T flow) throws Exception {
    flower.getFlowExec(flowType).runFlow(flow).getFuture().get(10, TimeUnit.SECONDS);
  }

  static void assertOn(String prefix, AtomicReference<String> threadName) {
    String name = threadName.get();
    assertTrue(name != null && name.startsWith(prefix + "-"), "ran on thread [" + name + "]");
  }

  @Test
  void transitAndEventsAfterExternallyCompletedStep_runOnFlowerThread() throws Exception {
    TA_AsyncStepFlow.transitThread.set(null);
    TA_AsyncStepFlow.afterExecThread.set(null);
    Flower flower = flower(namedScheduler("flowerA"), TA_AsyncStepFlow.class, TA_AfterExecProfile.class);
    try {
      run(flower, TA_AsyncStepFlow.class, new TA_AsyncStepFlow());
      assertOn("flowerA", TA_AsyncStepFlow.transitThread);
      assertOn("flowerA", TA_AsyncStepFlow.afterExecThread);
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void stepAfterExternallyCompletedEventFunction_runsOnFlowerThread() throws Exception {
    TA_AsyncEventFlow.stepThread.set(null);
    Flower flower =
        flower(namedScheduler("flowerA"), TA_AsyncEventFlow.class, TA_AsyncBeforeExecProfile.class);
    try {
      run(flower, TA_AsyncEventFlow.class, new TA_AsyncEventFlow());
      assertOn("flowerA", TA_AsyncEventFlow.stepThread);
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void synchronizedAfterExternallyCompletedBlocking_runsOnFlowerThread() throws Exception {
    TA_BlockingThenSynchronizedProfile.synchronizedThread.set(null);
    Flower flower =
        flower(
            namedScheduler("flowerA"),
            TA_BlockingFlow.class,
            TA_BlockingThenSynchronizedProfile.class);
    try {
      run(flower, TA_BlockingFlow.class, new TA_BlockingFlow());
      assertOn("flowerA", TA_BlockingThenSynchronizedProfile.synchronizedThread);
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void twoEngines_stageCompletedOnOtherEnginesThread_runsOnOwnThread() throws Exception {
    TA_CrossEngineFlow.completingThread.set(null);
    TA_CrossEngineFlow.transitThread.set(null);
    ScheduledExecutorService schedulerA = namedScheduler("flowerA");
    Flower flowerA = flower(schedulerA, TA_AsyncStepFlow.class, TA_AfterExecProfile.class);
    Flower flowerB = flower(namedScheduler("flowerB"), TA_CrossEngineFlow.class);
    try {
      // Engine A runs a Flow, so its threads are marked as A's scheduler threads
      run(flowerA, TA_AsyncStepFlow.class, new TA_AsyncStepFlow());

      // B's Step returns a future that is completed on one of A's threads
      TA_CrossEngineFlow.otherScheduler.set(schedulerA);
      run(flowerB, TA_CrossEngineFlow.class, new TA_CrossEngineFlow());
      assertOn("flowerA", TA_CrossEngineFlow.completingThread);
      assertOn("flowerB", TA_CrossEngineFlow.transitThread);
    } finally {
      flowerA.shutdownScheduler();
      flowerB.shutdownScheduler();
    }
  }
}

@FlowType(name = "Test", firstStep = "step")
@EventProfiles({TA_AfterExecProfile.class})
class TA_AsyncStepFlow {
  static final AtomicReference<String> transitThread = new AtomicReference<>();
  static final AtomicReference<String> afterExecThread = new AtomicReference<>();

  @StepFunction(transit = "transit")
  static CompletableFuture<String> step() {
    return ThreadAffinityTest.completeExternally("done");
  }

  @TransitFunction
  static Transition transit(@Terminal Transition end) {
    transitThread.set(Thread.currentThread().getName());
    return end;
  }
}

@EventProfileContainer(name = "TA_AfterExecProfile")
class TA_AfterExecProfile {
  public TA_AfterExecProfile() {}

  // SYNCHRONIZED in a single profile is called without being submitted to the scheduler
  @EventFunction(types = EventType.AFTER_EXEC, concurrency = Concurrency.SYNCHRONIZED)
  static void afterExec() {
    TA_AsyncStepFlow.afterExecThread.set(Thread.currentThread().getName());
  }
}

@FlowType(name = "Test", firstStep = "step")
@EventProfiles({TA_AsyncBeforeExecProfile.class})
class TA_AsyncEventFlow {
  static final AtomicReference<String> stepThread = new AtomicReference<>();

  @SimpleStepFunction
  static Transition step(@Terminal Transition end) {
    stepThread.set(Thread.currentThread().getName());
    return end;
  }
}

@EventProfileContainer(name = "TA_AsyncBeforeExecProfile")
class TA_AsyncBeforeExecProfile {
  public TA_AsyncBeforeExecProfile() {}

  @EventFunction(types = EventType.BEFORE_EXEC, concurrency = Concurrency.SYNCHRONIZED)
  static CompletableFuture<Void> beforeExec() {
    return ThreadAffinityTest.completeExternally(null);
  }
}

@FlowType(name = "Test", firstStep = "step")
@EventProfiles({TA_BlockingThenSynchronizedProfile.class})
class TA_BlockingFlow {
  @SimpleStepFunction
  static Transition step(@Terminal Transition end) {
    return end;
  }
}

@EventProfileContainer(name = "TA_BlockingThenSynchronizedProfile")
class TA_BlockingThenSynchronizedProfile {
  static final AtomicReference<String> synchronizedThread = new AtomicReference<>();

  public TA_BlockingThenSynchronizedProfile() {}

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.BLOCKING)
  static CompletableFuture<Void> blocking() {
    return ThreadAffinityTest.completeExternally(null);
  }

  @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.SYNCHRONIZED)
  static void synchronizedFunction() {
    synchronizedThread.set(Thread.currentThread().getName());
  }
}

@FlowType(name = "Test", firstStep = "step")
class TA_CrossEngineFlow {
  static final AtomicReference<ScheduledExecutorService> otherScheduler = new AtomicReference<>();
  static final AtomicReference<String> completingThread = new AtomicReference<>();
  static final AtomicReference<String> transitThread = new AtomicReference<>();

  @StepFunction(transit = "transit")
  static CompletableFuture<String> step() {
    CompletableFuture<String> future = new CompletableFuture<>();
    otherScheduler
        .get()
        .schedule(
            () -> {
              completingThread.set(Thread.currentThread().getName());
              future.complete("done");
            },
            50,
            TimeUnit.MILLISECONDS);
    return future;
  }

  @TransitFunction
  static Transition transit(@Terminal Transition end) {
    transitThread.set(Thread.currentThread().getName());
    return end;
  }
}
