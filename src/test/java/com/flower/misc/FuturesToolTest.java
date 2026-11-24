package com.flower.misc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.flower.utilities.FuturesTool;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FuturesToolTest {
  static final String EXECUTOR_THREAD = "futures-tool-test-executor";

  static class MyTestException extends RuntimeException {
    MyTestException(String message) {
      super(message);
    }
  }

  static class OtherTestException extends RuntimeException {}

  ExecutorService executor;
  final Executor rejectingExecutor =
      command -> {
        throw new RejectedExecutionException("rejected");
      };

  @BeforeEach
  void setUp() {
    executor = Executors.newSingleThreadExecutor(r -> new Thread(r, EXECUTOR_THREAD));
  }

  @AfterEach
  void tearDown() {
    executor.shutdownNow();
  }

  /** Raw failure of a future (unlike get(), does not unwrap CompletionException). */
  static Throwable failureOf(CompletableFuture<?> future) throws Exception {
    return future.handle((v, t) -> t).get(5, TimeUnit.SECONDS);
  }

  static <T> T getValue(CompletableFuture<T> future) throws Exception {
    return future.get(5, TimeUnit.SECONDS);
  }

  /** A future that fails with CompletionException(exception), as dependent stages do. */
  static <T> CompletableFuture<T> dependentFailure(Throwable exception) {
    return CompletableFuture.<T>failedFuture(exception).thenApply(v -> v);
  }

  static void assertOnExecutorThread() {
    assertEquals(EXECUTOR_THREAD, Thread.currentThread().getName());
  }

  // ---------------- unwrap / directExecutor

  @Test
  void unwrap_stripsNestedCompletionExceptions() {
    MyTestException exception = new MyTestException("x");
    assertSame(
        exception,
        FuturesTool.unwrap(new CompletionException(new CompletionException(exception))));
  }

  @Test
  void unwrap_keepsOtherExceptions() {
    MyTestException exception = new MyTestException("x");
    assertSame(exception, FuturesTool.unwrap(exception));
    ExecutionException executionException = new ExecutionException(exception);
    assertSame(executionException, FuturesTool.unwrap(executionException));
  }

  @Test
  void unwrap_keepsCompletionExceptionWithoutCause() {
    CompletionException exception = new CompletionException("no cause", null);
    assertSame(exception, FuturesTool.unwrap(exception));
  }

  @Test
  void directExecutor_runsInCallingThread() {
    Thread caller = Thread.currentThread();
    AtomicReference<Thread> ran = new AtomicReference<>();
    FuturesTool.directExecutor().execute(() -> ran.set(Thread.currentThread()));
    assertSame(caller, ran.get());
  }

  // ---------------- transform / transformAsync

  @Test
  void transform_appliesFunctionOnExecutor() throws Exception {
    CompletableFuture<Integer> input = new CompletableFuture<>();
    CompletableFuture<String> result =
        FuturesTool.transform(
            input,
            i -> {
              assertOnExecutorThread();
              return "v" + i;
            },
            executor);
    input.complete(1);
    assertEquals("v1", getValue(result));
  }

  @Test
  void transform_inputFailure_propagatesUnwrapped() throws Exception {
    MyTestException exception = new MyTestException("x");
    CompletableFuture<String> result =
        FuturesTool.transform(dependentFailure(exception), i -> "v" + i, executor);
    assertSame(exception, failureOf(result));
  }

  @Test
  void transform_functionThrows_failsWithThatException() throws Exception {
    MyTestException exception = new MyTestException("x");
    CompletableFuture<String> result =
        FuturesTool.transform(
            CompletableFuture.completedFuture(1),
            i -> {
              throw exception;
            },
            executor);
    assertSame(exception, failureOf(result));
  }

  @Test
  void transform_executorRejects_failsWithRejection() throws Exception {
    CompletableFuture<String> result =
        FuturesTool.transform(CompletableFuture.completedFuture(1), i -> "v" + i, rejectingExecutor);
    assertEquals(RejectedExecutionException.class, failureOf(result).getClass());
  }

  @Test
  void transformAsync_composesFutures() throws Exception {
    CompletableFuture<Integer> inner = new CompletableFuture<>();
    CompletableFuture<Integer> result =
        FuturesTool.transformAsync(
            CompletableFuture.completedFuture(1),
            i -> {
              assertOnExecutorThread();
              return inner;
            },
            executor);
    Thread.sleep(20);
    assertFalse(result.isDone());
    inner.complete(2);
    assertEquals(2, getValue(result));
  }

  @Test
  void transformAsync_innerFailure_propagatesUnwrapped() throws Exception {
    MyTestException exception = new MyTestException("x");
    CompletableFuture<Integer> result =
        FuturesTool.transformAsync(
            CompletableFuture.completedFuture(1), i -> dependentFailure(exception), executor);
    assertSame(exception, failureOf(result));
  }

  @Test
  void transformAsync_functionReturnsNull_failsWithNpe() throws Exception {
    CompletableFuture<Integer> result =
        FuturesTool.transformAsync(CompletableFuture.completedFuture(1), i -> null, executor);
    assertEquals(NullPointerException.class, failureOf(result).getClass());
  }

  @Test
  void transformAsync_executorRejects_failsWithRejection() throws Exception {
    CompletableFuture<Integer> result =
        FuturesTool.transformAsync(
            CompletableFuture.completedFuture(1),
            CompletableFuture::completedFuture,
            rejectingExecutor);
    assertEquals(RejectedExecutionException.class, failureOf(result).getClass());
  }

  // ---------------- catching / catchingAsync

  @Test
  void catching_success_passesValueThrough() throws Exception {
    CompletableFuture<Integer> result =
        FuturesTool.catching(
            CompletableFuture.completedFuture(1), MyTestException.class, e -> -1, executor);
    assertEquals(1, getValue(result));
  }

  @Test
  void catching_matchingException_appliesFallbackToUnwrappedException() throws Exception {
    MyTestException exception = new MyTestException("x");
    AtomicReference<Throwable> caught = new AtomicReference<>();
    CompletableFuture<Integer> result =
        FuturesTool.catching(
            FuturesToolTest.<Integer>dependentFailure(exception),
            MyTestException.class,
            e -> {
              assertOnExecutorThread();
              caught.set(e);
              return -1;
            },
            executor);
    assertEquals(-1, getValue(result));
    assertSame(exception, caught.get());
  }

  @Test
  void catching_nonMatchingException_propagatesUnwrapped() throws Exception {
    OtherTestException exception = new OtherTestException();
    CompletableFuture<Integer> result =
        FuturesTool.catching(
            FuturesToolTest.<Integer>dependentFailure(exception),
            MyTestException.class,
            e -> -1,
            executor);
    assertSame(exception, failureOf(result));
  }

  @Test
  void catching_fallbackThrows_failsWithThatException() throws Exception {
    OtherTestException fallbackException = new OtherTestException();
    CompletableFuture<Integer> result =
        FuturesTool.catching(
            CompletableFuture.<Integer>failedFuture(new MyTestException("x")),
            MyTestException.class,
            e -> {
              throw fallbackException;
            },
            executor);
    assertSame(fallbackException, failureOf(result));
  }

  @Test
  void catching_throwable_catchesCancellation() throws Exception {
    CompletableFuture<Integer> input = new CompletableFuture<>();
    input.cancel(false);
    AtomicReference<Throwable> caught = new AtomicReference<>();
    CompletableFuture<Integer> result =
        FuturesTool.catching(
            input,
            Throwable.class,
            e -> {
              caught.set(e);
              return -1;
            },
            executor);
    assertEquals(-1, getValue(result));
    assertTrue(caught.get() instanceof CancellationException);
  }

  @Test
  void catchingAsync_matchingException_composesFallbackFuture() throws Exception {
    MyTestException exception = new MyTestException("x");
    CompletableFuture<Integer> result =
        FuturesTool.catchingAsync(
            FuturesToolTest.<Integer>dependentFailure(exception),
            MyTestException.class,
            e -> {
              assertOnExecutorThread();
              assertSame(exception, e);
              return CompletableFuture.completedFuture(-1);
            },
            executor);
    assertEquals(-1, getValue(result));
  }

  @Test
  void catchingAsync_fallbackFutureFails_failsUnwrapped() throws Exception {
    OtherTestException fallbackException = new OtherTestException();
    CompletableFuture<Integer> result =
        FuturesTool.catchingAsync(
            CompletableFuture.<Integer>failedFuture(new MyTestException("x")),
            MyTestException.class,
            e -> dependentFailure(fallbackException),
            executor);
    assertSame(fallbackException, failureOf(result));
  }

  @Test
  void catchingAsync_nonMatchingException_propagatesUnwrapped() throws Exception {
    OtherTestException exception = new OtherTestException();
    CompletableFuture<Integer> result =
        FuturesTool.catchingAsync(
            FuturesToolTest.<Integer>dependentFailure(exception),
            MyTestException.class,
            e -> CompletableFuture.completedFuture(-1),
            executor);
    assertSame(exception, failureOf(result));
  }

  // ---------------- tryCatch / tryCatchAsync

  @Test
  void tryCatch_successAndFailure() throws Exception {
    assertEquals(
        "v1",
        getValue(
            FuturesTool.tryCatch(
                CompletableFuture.completedFuture(1),
                i -> "v" + i,
                MyTestException.class,
                e -> "fallback",
                executor)));
    assertEquals(
        "fallback",
        getValue(
            FuturesTool.tryCatch(
                CompletableFuture.completedFuture(1),
                i -> {
                  throw new MyTestException("x");
                },
                MyTestException.class,
                e -> "fallback",
                executor)));
  }

  @Test
  void tryCatchAsync_successAndFailure() throws Exception {
    assertEquals(
        "v1",
        getValue(
            FuturesTool.tryCatchAsync(
                CompletableFuture.completedFuture(1),
                i -> CompletableFuture.completedFuture("v" + i),
                MyTestException.class,
                e -> CompletableFuture.completedFuture("fallback"),
                executor)));
    assertEquals(
        "fallback",
        getValue(
            FuturesTool.tryCatchAsync(
                CompletableFuture.completedFuture(1),
                i -> dependentFailure(new MyTestException("x")),
                MyTestException.class,
                e -> CompletableFuture.completedFuture("fallback"),
                executor)));
  }

  // ---------------- whenAllComplete*

  @Test
  void whenAllComplete_waitsForAllAndIgnoresFailures() throws Exception {
    CompletableFuture<Integer> f1 = new CompletableFuture<>();
    CompletableFuture<Integer> f2 = new CompletableFuture<>();
    CompletableFuture<Void> all = FuturesTool.whenAllComplete(f1, f2);

    f1.completeExceptionally(new MyTestException("x"));
    assertFalse(all.isDone());
    f2.complete(2);
    assertNull(getValue(all));
  }

  @Test
  void whenAllComplete_empty_completesImmediately() throws Exception {
    assertTrue(FuturesTool.whenAllComplete(List.of()).isDone());
  }

  @Test
  void whenAllCompleteCall_runsCombinerOnExecutorAfterAllDone() throws Exception {
    CompletableFuture<Integer> f1 = new CompletableFuture<>();
    CompletableFuture<Integer> f2 = new CompletableFuture<>();
    CompletableFuture<Integer> result =
        FuturesTool.whenAllCompleteCall(
            List.of(f1, f2),
            () -> {
              assertOnExecutorThread();
              return f1.join() + f2.join();
            },
            executor);

    f1.complete(1);
    Thread.sleep(20);
    assertFalse(result.isDone());
    f2.complete(2);
    assertEquals(3, getValue(result));
  }

  @Test
  void whenAllCompleteCall_combinerThrowsCheckedException_failsWithIt() throws Exception {
    IOException exception = new IOException("x");
    CompletableFuture<Integer> result =
        FuturesTool.whenAllCompleteCall(
            List.of(CompletableFuture.completedFuture(1)),
            () -> {
              throw exception;
            },
            executor);
    assertSame(exception, failureOf(result));
  }

  @Test
  void whenAllCompleteRun_runsAfterFailures() throws Exception {
    AtomicBoolean ran = new AtomicBoolean(false);
    CompletableFuture<Void> result =
        FuturesTool.whenAllCompleteRun(
            List.of(CompletableFuture.failedFuture(new MyTestException("x"))),
            () -> {
              assertOnExecutorThread();
              ran.set(true);
            },
            executor);
    assertNull(getValue(result));
    assertTrue(ran.get());
  }

  @Test
  void whenAllCompleteRun_executorRejects_failsWithRejection() throws Exception {
    CompletableFuture<Void> result =
        FuturesTool.whenAllCompleteRun(
            List.of(CompletableFuture.completedFuture(1)), () -> {}, rejectingExecutor);
    assertEquals(RejectedExecutionException.class, failureOf(result).getClass());
  }

  // ---------------- whenAnyComplete*

  @Test
  void whenAnyComplete_firstSuccessWins() throws Exception {
    CompletableFuture<Integer> f1 = new CompletableFuture<>();
    CompletableFuture<Integer> f2 = new CompletableFuture<>();
    CompletableFuture<Integer> any = FuturesTool.whenAnyComplete(List.of(f1, f2), executor);

    f2.complete(2);
    assertEquals(2, getValue(any));
    f1.complete(1);
    assertEquals(2, getValue(any));
  }

  @Test
  void whenAnyComplete_firstFailureWins_unwrapped() throws Exception {
    MyTestException exception = new MyTestException("x");
    CompletableFuture<Integer> f1 = dependentFailure(exception);
    CompletableFuture<Integer> f2 = new CompletableFuture<>();
    CompletableFuture<Integer> any = FuturesTool.whenAnyComplete(List.of(f1, f2), executor);
    assertSame(exception, failureOf(any));
  }

  @Test
  void whenAnyCompleteIgnoreResult_completesOnFailure() throws Exception {
    CompletableFuture<Integer> f1 = new CompletableFuture<>();
    CompletableFuture<Integer> f2 = new CompletableFuture<>();
    CompletableFuture<Void> any =
        FuturesTool.whenAnyCompleteIgnoreResult(List.of(f1, f2), executor);

    assertFalse(any.isDone());
    f1.completeExceptionally(new MyTestException("x"));
    assertNull(getValue(any));
  }

  // ---------------- assignSettableFuture

  @Test
  void assignSettableFuture_success() throws Exception {
    CompletableFuture<Integer> source = new CompletableFuture<>();
    CompletableFuture<Integer> target = new CompletableFuture<>();
    AtomicReference<String> completedOn = new AtomicReference<>();
    CompletableFuture<Void> observed =
        target.thenAccept(v -> completedOn.set(Thread.currentThread().getName()));

    FuturesTool.assignSettableFuture(source, target, executor);
    assertFalse(target.isDone());
    source.complete(1);
    assertEquals(1, getValue(target));
    getValue(observed);
    assertEquals(EXECUTOR_THREAD, completedOn.get());
  }

  @Test
  void assignSettableFuture_failure_unwrapped() throws Exception {
    MyTestException exception = new MyTestException("x");
    CompletableFuture<Integer> target = new CompletableFuture<>();
    FuturesTool.assignSettableFuture(dependentFailure(exception), target, executor);
    assertSame(exception, failureOf(target));
  }

  @Test
  void assignSettableFuture_executorRejects_failsTarget() throws Exception {
    CompletableFuture<Integer> target = new CompletableFuture<>();
    FuturesTool.assignSettableFuture(
        CompletableFuture.completedFuture(1), target, rejectingExecutor);
    assertEquals(RejectedExecutionException.class, failureOf(target).getClass());
  }

  /** Chaining many steps through assignSettableFuture must not overflow the stack. */
  @Test
  void assignSettableFuture_longDecoupledChain_doesNotOverflow() throws Exception {
    CompletableFuture<Integer> done = new CompletableFuture<>();
    step(0, 100_000, done);
    assertEquals(100_000, done.get(60, TimeUnit.SECONDS));
  }

  void step(int i, int max, CompletableFuture<Integer> done) {
    if (i == max) {
      done.complete(i);
      return;
    }
    CompletableFuture<Integer> next = new CompletableFuture<>();
    CompletableFuture<?> ignored =
        next.thenAccept(v -> step(v, max, done));
    FuturesTool.assignSettableFuture(CompletableFuture.completedFuture(i + 1), next, executor);
  }
}
