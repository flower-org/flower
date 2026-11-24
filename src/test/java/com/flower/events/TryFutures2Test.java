package com.flower.events;

import static com.flower.utilities.FuturesTool.directExecutor;

import com.flower.utilities.FuturesTool;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class TryFutures2Test {
  CompletableFuture<Integer> success() {
    return CompletableFuture.completedFuture(123);
  }

  CompletableFuture<Integer> failure() {
    return CompletableFuture.failedFuture(new Exception("OOPS"));
  }

  // @Test
  public void tesAllSuccess1() throws InterruptedException {
    CompletableFuture<Integer> future1 = success();
    CompletableFuture<Integer> future2 = success();
    CompletableFuture<Integer> future3 = success();

    // allOf fails if any input fails, so the callback runs only when all succeed (whenAllSucceed)
    CompletableFuture<Void> unused =
        CompletableFuture.allOf(future1, future2, future3)
            .thenRunAsync(() -> System.out.println("weew 1"), directExecutor());

    Thread.sleep(10000);
  }

  // @Test
  public void tesAllSuccess2() throws InterruptedException {
    CompletableFuture<Integer> future1 = success();
    CompletableFuture<Integer> future2 = success();
    CompletableFuture<Integer> future3 = failure();

    CompletableFuture<Void> unused =
        CompletableFuture.allOf(future1, future2, future3)
            .thenRunAsync(() -> System.out.println("weew 2"), directExecutor());

    Thread.sleep(10000);
  }

  // @Test
  public void tesAllSuccess3() throws InterruptedException {
    CompletableFuture<Integer> future1 = success();
    CompletableFuture<Integer> future2 = success();
    CompletableFuture<Integer> future3 = failure();

    CompletableFuture<Void> unused =
        FuturesTool.whenAllCompleteRun(
            List.of(future1, future2, future3),
            () -> System.out.println("weew 3"),
            directExecutor());

    Thread.sleep(10000);
  }
}
