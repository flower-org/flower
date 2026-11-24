package com.flower.misc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.flower.utilities.FutureCombiner;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class FutureCombinerTest {
  private static final String EXCEPTION_MESSAGE = "FutureCombiner test exception";

  static class MyTestException extends RuntimeException {
    public MyTestException(String message) {
      super(message);
    }
  }

  final int num1 = 123, num2 = 456;

  ScheduledExecutorService scheduler;

  @BeforeEach
  void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  static Throwable failureOf(CompletableFuture<?> future) throws Exception {
    return future.handle((v, t) -> t).get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testChainedFutures_Future1() throws Exception {
    CompletableFuture<Integer> future = CompletableFuture.completedFuture(func(num1));

    CompletableFuture<List<Integer>> list = FutureCombiner.chainResults(future);
    assertEquals(1, list.get().size());
    assertTrue(list.get().contains(num1));
  }

  @Test
  public void testChainedFutures_FutureWithList() throws Exception {
    CompletableFuture<Integer> future1 = CompletableFuture.completedFuture(func(num1));
    CompletableFuture<Integer> future2 = CompletableFuture.completedFuture(func(num2));
    CompletableFuture<List<Integer>> list = FutureCombiner.chainResults(future2);

    CompletableFuture<List<Integer>> resultList = FutureCombiner.chainResults(future1, list);
    assertEquals(ImmutableList.of(num2, num1), resultList.get());
  }

  @Test
  public void testChainedFutures_FutureWithNullList() throws Exception {
    CompletableFuture<Integer> future = CompletableFuture.completedFuture(func(num1));

    CompletableFuture<List<Integer>> resultList = FutureCombiner.chainResults(future, null);
    assertEquals(ImmutableList.of(num1), resultList.get());
  }

  @Test
  public void testChainedFutures_waitsForBothFutures() throws Exception {
    CompletableFuture<Integer> future = new CompletableFuture<>();
    CompletableFuture<List<Integer>> list = new CompletableFuture<>();

    CompletableFuture<List<Integer>> resultList = FutureCombiner.chainResults(future, list);
    future.complete(num1);
    assertFalse(resultList.isDone());
    list.complete(new ArrayList<>(List.of(num2)));
    assertEquals(ImmutableList.of(num2, num1), resultList.get(5, TimeUnit.SECONDS));
  }

  @Test
  public void testChainedFutures_failedFuture_failsWithOriginalException() throws Exception {
    MyTestException exception = new MyTestException(EXCEPTION_MESSAGE);
    CompletableFuture<Integer> future = CompletableFuture.failedFuture(exception);

    assertSame(exception, failureOf(FutureCombiner.chainResults(future)));
  }

  @Test
  public void testChainedFutures_ListWithList() throws Exception {
    CompletableFuture<List<Integer>> list1 =
        FutureCombiner.chainResults(CompletableFuture.completedFuture(func(num1)));
    CompletableFuture<List<Integer>> list2 =
        FutureCombiner.chainResults(CompletableFuture.completedFuture(func(num2)));

    CompletableFuture<List<Integer>> resultList = FutureCombiner.chainListResults(list1, list2);
    assertEquals(2, resultList.get().size());
    assertTrue(resultList.get().contains(num1));
    assertTrue(resultList.get().contains(num2));
  }

  @Test
  public void testChainedFutures_ListWithNullList() throws Exception {
    CompletableFuture<List<Integer>> list =
        FutureCombiner.chainResults(CompletableFuture.completedFuture(func(num1)));

    CompletableFuture<List<Integer>> resultList = FutureCombiner.chainListResults(list, null);
    assertEquals(ImmutableList.of(num1), resultList.get());
  }

  @Test
  public void testChainedFutures_NullListWithList() throws Exception {
    CompletableFuture<List<Integer>> list =
        FutureCombiner.chainResults(CompletableFuture.completedFuture(func(num2)));
    CompletableFuture<List<Integer>> resultList = FutureCombiner.chainListResults(null, list);
    assertEquals(ImmutableList.of(num2), resultList.get());
  }

  @Test
  public void testChainedFutures_NullListWithNullList() throws Exception {
    CompletableFuture<List<Integer>> list = FutureCombiner.chainListResults(null, null);
    assertTrue(list.get().isEmpty());
  }

  @Test
  public void testChainedFutures_varArgs_shouldConcatenateFutures() throws Exception {
    CompletableFuture<List<Integer>> list1 =
        FutureCombiner.chainResults(CompletableFuture.completedFuture(func(num1)));
    CompletableFuture<List<Integer>> list2 =
        FutureCombiner.chainResults(CompletableFuture.completedFuture(func(num2)));

    List<Integer> resultList = FutureCombiner.chainListResults(null, list1, list2).get();
    assertEquals(ImmutableList.of(num1, num2), resultList);
  }

  @Test
  public void testChainedFutures_varArgsEmpty_shouldReturnEmptyList() throws Exception {
    assertTrue(FutureCombiner.<Integer>chainListResults().get().isEmpty());
  }

  @Test
  public void testChainedFutures_varArgsWhenAFutureFails_shouldReturnFailedFuture()
      throws Exception {
    MyTestException exception = new MyTestException(EXCEPTION_MESSAGE);
    CompletableFuture<List<Integer>> list1 =
        FutureCombiner.chainResults(CompletableFuture.completedFuture(func(num1)));
    CompletableFuture<List<Integer>> list2 =
        FutureCombiner.chainResults(CompletableFuture.failedFuture(exception));

    CompletableFuture<List<Integer>> result = FutureCombiner.chainListResults(null, list1, list2);
    assertThrows(ExecutionException.class, result::get);
    assertSame(exception, failureOf(result));
  }

  @Test
  public void testCollapseNestedFuture() throws Exception {
    CompletableFuture<Integer> inner = new CompletableFuture<>();
    CompletableFuture<Integer> collapsed =
        FutureCombiner.collapseNestedFuture(CompletableFuture.completedFuture(inner));
    assertFalse(collapsed.isDone());
    inner.complete(num1);
    assertEquals(num1, collapsed.get(5, TimeUnit.SECONDS));
  }

  @Test
  public void testDelayedExecution() throws Exception {
    final int val = 324;
    CompletableFuture<Integer> future =
        FutureCombiner.delayExecution(scheduler, 10, TimeUnit.MILLISECONDS, () -> func(val));
    assertEquals(val, future.get(5, TimeUnit.SECONDS));
  }

  @Test
  public void testDelayedExecutionAsync() throws Exception {
    final int val = 324;
    CompletableFuture<Integer> future =
        FutureCombiner.delayExecutionAsync(
            scheduler,
            10,
            TimeUnit.MILLISECONDS,
            () -> CompletableFuture.completedFuture(func(val)));
    assertEquals(val, future.get(5, TimeUnit.SECONDS));
  }

  @Test
  public void testDelayedExecution_isDelayed() throws Exception {
    CompletableFuture<Integer> future =
        FutureCombiner.delayExecution(scheduler, 1, TimeUnit.HOURS, () -> func(num1));
    assertThrows(TimeoutException.class, () -> future.get(50, TimeUnit.MILLISECONDS));
  }

  Void exceptionSync() {
    throw new MyTestException(EXCEPTION_MESSAGE);
  }

  @Test
  public void testDelayedExecutionException() throws Exception {
    CompletableFuture<Void> future =
        FutureCombiner.delayExecution(scheduler, 10, TimeUnit.MILLISECONDS, this::exceptionSync);

    ExecutionException e = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    assertEquals(MyTestException.class, e.getCause().getClass());
    assertEquals(EXCEPTION_MESSAGE, e.getCause().getMessage());
    assertEquals(MyTestException.class, failureOf(future).getClass());
  }

  @Test
  public void testDelayedExecutionAsyncException() throws Exception {
    CompletableFuture<Void> future =
        FutureCombiner.delayExecutionAsync(
            scheduler,
            10,
            TimeUnit.MILLISECONDS,
            () -> CompletableFuture.completedFuture(exceptionSync()));

    ExecutionException e = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    assertEquals(MyTestException.class, e.getCause().getClass());
    assertEquals(EXCEPTION_MESSAGE, e.getCause().getMessage());
    assertEquals(MyTestException.class, failureOf(future).getClass());
  }

  @Test
  public void testDelayedExecutionAsync_innerFutureFails() throws Exception {
    MyTestException exception = new MyTestException(EXCEPTION_MESSAGE);
    CompletableFuture<Void> future =
        FutureCombiner.delayExecutionAsync(
            scheduler, 10, TimeUnit.MILLISECONDS, () -> CompletableFuture.failedFuture(exception));

    assertSame(exception, failureOf(future));
  }

  @Test
  public void testDelayedExecution_rejected_failsFuture() throws Exception {
    scheduler.shutdownNow();
    CompletableFuture<Integer> future =
        FutureCombiner.delayExecution(scheduler, 10, TimeUnit.MILLISECONDS, () -> func(num1));

    assertEquals(RejectedExecutionException.class, failureOf(future).getClass());
  }

  @Test
  public void testDelayedExecution_cancel_cancelsScheduledTask() {
    ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
    ScheduledFuture<?> scheduledTask = mock(ScheduledFuture.class);
    Mockito.<ScheduledFuture<?>>when(
            mockScheduler.schedule(any(Runnable.class), eq(1L), eq(TimeUnit.HOURS)))
        .thenReturn(scheduledTask);

    CompletableFuture<Integer> future =
        FutureCombiner.delayExecution(mockScheduler, 1, TimeUnit.HOURS, () -> func(num1));
    future.cancel(false);

    verify(scheduledTask).cancel(false);
  }

  @Test
  public void testDelayedExecutionParameterPassing() throws Exception {
    ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
    ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);

    CompletableFuture<Integer> future1 =
        FutureCombiner.delayExecutionAsync(
            mockScheduler,
            10,
            TimeUnit.MICROSECONDS,
            () -> CompletableFuture.completedFuture(func(num1)));
    CompletableFuture<Integer> future2 =
        FutureCombiner.delayExecutionAsync(
            mockScheduler,
            20,
            TimeUnit.MILLISECONDS,
            () -> CompletableFuture.completedFuture(func(num2)));

    verify(mockScheduler).schedule(task.capture(), eq(10L), eq(TimeUnit.MICROSECONDS));
    verify(mockScheduler).schedule(task.capture(), eq(20L), eq(TimeUnit.MILLISECONDS));
    assertFalse(future1.isDone());
    assertFalse(future2.isDone());

    task.getAllValues().forEach(Runnable::run);
    assertEquals(num1, future1.get(5, TimeUnit.SECONDS));
    assertEquals(num2, future2.get(5, TimeUnit.SECONDS));
  }

  Integer func(int i) {
    return i;
  }
}
