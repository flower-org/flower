package com.flower.utilities;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/** Utility class - allows to combine Futures and execute functions with a delay. */
public class FutureCombiner {
  /**
   * Transforms a Future that returns a result to a Future that returns a List containing original
   * Future's result. Useful as a first step in chaining Futures.
   *
   * @param result Result Future
   * @param <T> Result type
   * @return Future that returns a List containing original Future's result.
   */
  public static <T> CompletableFuture<List<T>> chainResults(CompletableFuture<T> result) {
    return chainResults(result, null);
  }

  /**
   * Creates a Future that returns a List of results containing all elements of mainResultList plus
   * a result of nextResult
   *
   * @param nextResult Future that returns a single result
   * @param mainResultList Future that returns a List of results
   * @param <T> Result type
   * @return Future that returns mainResultList plus nextResult
   */
  public static <T> CompletableFuture<List<T>> chainResults(
      CompletableFuture<T> nextResult, @Nullable CompletableFuture<List<T>> mainResultList) {
    if (mainResultList == null) {
      return chainResults(nextResult, CompletableFuture.completedFuture(new ArrayList<>()));
    } else {
      return FuturesTool.whenAllCompleteCall(
          List.of(nextResult, mainResultList),
          () -> {
            List<T> list = getDone(mainResultList);
            T element = getDone(nextResult);
            list.add(element);
            return list;
          },
          FuturesTool.directExecutor());
    }
  }

  /**
   * Creates a Future that returns a List of results containing all elements of mainResultList and
   * all elements of nextResults
   *
   * @param nextResults Additional Future that returns a List of results
   * @param mainResultList Main Future that returns a List of results
   * @param <T> Result type
   * @return Future that returns mainResultList plus nextResults
   */
  public static <T> CompletableFuture<List<T>> chainListResults(
      @Nullable CompletableFuture<List<T>> nextResults,
      @Nullable CompletableFuture<List<T>> mainResultList) {
    if (mainResultList == null && nextResults == null)
      return CompletableFuture.completedFuture(new ArrayList<>());
    else if (mainResultList == null) return Preconditions.checkNotNull(nextResults);
    else if (nextResults == null) return Preconditions.checkNotNull(mainResultList);
    else {
      return FuturesTool.whenAllCompleteCall(
          List.of(nextResults, mainResultList),
          () -> {
            List<T> list1 = getDone(mainResultList);
            List<T> list2 = getDone(nextResults);
            list1.addAll(list2);
            return list1;
          },
          FuturesTool.directExecutor());
    }
  }

  /**
   * Creates a Future that returns a List of results containing all elements of all futures passed
   * as an argument. Fails if any of the futures fails.
   *
   * @param futures Futures that should be combined (null elements are ignored)
   * @param <T> Result type
   * @return Future that returns a list with elements from all futures
   */
  @SafeVarargs
  public static <T> CompletableFuture<List<T>> chainListResults(
      CompletableFuture<List<T>>... futures) {
    ImmutableList<CompletableFuture<List<T>>> futuresList =
        Arrays.stream(futures).filter(Objects::nonNull).collect(ImmutableList.toImmutableList());

    if (futuresList.isEmpty()) {
      return CompletableFuture.completedFuture(ImmutableList.of());
    }

    return FuturesTool.transform(
        CompletableFuture.allOf(futuresList.toArray(new CompletableFuture<?>[0])),
        ignored -> {
          ImmutableList.Builder<T> resultsBuilder = ImmutableList.builder();
          for (CompletableFuture<List<T>> future : futuresList) {
            resultsBuilder.addAll(future.join());
          }
          return resultsBuilder.build();
        },
        FuturesTool.directExecutor());
  }

  /**
   * Transforms a Future with a nested Future into a single Future.
   *
   * @param future Future that returns another Future that returns a result
   * @param <T> Result type
   * @return Future that returns a result
   */
  public static <T> CompletableFuture<T> collapseNestedFuture(
      CompletableFuture<CompletableFuture<T>> future) {
    return FuturesTool.transformAsync(
        future, nestedFuture -> nestedFuture, FuturesTool.directExecutor());
  }

  /**
   * Executes an asynchronous function with a delay
   *
   * @param scheduler Scheduler
   * @param delay Delay
   * @param timeUnit Delay time unit
   * @param callable Asynchronous function to execute with a delay
   * @param <T> Result type
   * @return Future of delayed function execution
   */
  public static <T> CompletableFuture<T> delayExecutionAsync(
      ScheduledExecutorService scheduler,
      long delay,
      TimeUnit timeUnit,
      Callable<CompletableFuture<T>> callable) {
    return collapseNestedFuture(delayExecution(scheduler, delay, timeUnit, callable));
  }

  /**
   * Executes a function with a delay. Cancelling the returned Future cancels the scheduled task.
   *
   * @param scheduler Scheduler
   * @param delay Delay
   * @param timeUnit Delay time unit
   * @param callable Function to execute with a delay
   * @param <T> Result type
   * @return Future of delayed function execution
   */
  public static <T> CompletableFuture<T> delayExecution(
      ScheduledExecutorService scheduler, long delay, TimeUnit timeUnit, Callable<T> callable) {
    CompletableFuture<T> result = new CompletableFuture<>();
    ScheduledFuture<?> scheduledTask;
    try {
      scheduledTask =
          scheduler.schedule(
              () -> {
                try {
                  result.complete(callable.call());
                } catch (Throwable t) {
                  result.completeExceptionally(t);
                }
              },
              delay,
              timeUnit);
    } catch (Throwable t) {
      result.completeExceptionally(t);
      return result;
    }
    CompletableFuture<?> ignored =
        result.whenComplete(
        (v, t) -> {
          if (result.isCancelled()) {
            scheduledTask.cancel(false);
          }
        });
    return result;
  }

  /** Value of an already completed future; failures are rethrown unwrapped. */
  private static <T> T getDone(CompletableFuture<T> future) throws Exception {
    try {
      return future.join();
    } catch (CompletionException e) {
      Throwable cause = FuturesTool.unwrap(e);
      if (cause instanceof Exception) {
        throw (Exception) cause;
      } else if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw e;
    }
  }
}
