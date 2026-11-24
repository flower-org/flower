package com.flower.ooms;

import com.flower.executor.TaskCheckingExecutor;
import com.flower.utilities.FuturesTool;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

public class RecursiveFutureOOMExample {
  static CompletableFuture<Integer> recursion(
      CompletableFuture<Integer> previous, Executor executor) {
    return FuturesTool.transformAsync(
        previous,
        i -> {
          System.out.println(i);
          if (i == Integer.MAX_VALUE) {
            return CompletableFuture.completedFuture(i);
          } else {
            return recursion(CompletableFuture.completedFuture(i + 1), executor);
          }
        },
        executor);
  }

  static void recursionDecoupled(
      CompletableFuture<Integer> previous, Executor executor, CompletableFuture<Integer> set) {
    CompletableFuture<Void> unused =
        FuturesTool.whenAllCompleteRun(
            List.of(previous),
            () -> {
              Integer i;
              try {
                i = previous.get();
                System.out.println(i);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }

              if (i == Integer.MAX_VALUE) {
                set.complete(i);
              } else {
                recursionDecoupled(CompletableFuture.completedFuture(i + 1), executor, set);
              }
            },
            executor);
  }

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    ScheduledExecutorService executor = TaskCheckingExecutor.builder().corePoolSize(1).build();

    // TODO: THIS OOMs (recursion)
    // uncomment for testing
    //        CompletableFuture<Integer> iFuture =
    //            recursion(CompletableFuture.completedFuture(1), executor);

    // TODO: THIS doesn't OOM (recursion)
    // uncomment for testing
    CompletableFuture<Integer> iFuture = new CompletableFuture<>();
    recursionDecoupled(CompletableFuture.completedFuture(1), executor, iFuture);

    System.out.println(iFuture.get());
  }
}
