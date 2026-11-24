package com.flower.events;

import static com.flower.utilities.FuturesTool.directExecutor;

import com.flower.utilities.FuturesTool;
import java.util.concurrent.CompletableFuture;

public class TryFuturesTest {
  CompletableFuture<Integer> success() {
    return CompletableFuture.completedFuture(123);
  }

  CompletableFuture<Integer> failure() {
    return CompletableFuture.failedFuture(new Exception("OOPS"));
  }

  // Plain but cumbersome way
  CompletableFuture<Integer> transformCall(CompletableFuture<Integer> future, String funcName) {
    return FuturesTool.transformAsync(
        FuturesTool.catchingAsync(
            future,
            Throwable.class,
            t -> {
              System.out.printf("Exception caught %s - %s\n", funcName, t.getMessage());
              return CompletableFuture.failedFuture(t);
            },
            directExecutor()),
        i -> {
          System.out.printf("Calling %s - %d\n", funcName, i);
          return CompletableFuture.completedFuture(i);
        },
        directExecutor());
  }

  // Fluent syntax, looks more like try / catch.
  // Note: native CompletableFuture stages wrap failures in CompletionException, hence unwrap().
  CompletableFuture<Integer> transformCall2(CompletableFuture<Integer> future, String funcName) {
    return future
        .thenComposeAsync(
            i -> {
              System.out.printf("Calling %s - %d\n", funcName, i);
              return CompletableFuture.completedFuture(i);
            },
            directExecutor())
        .exceptionallyComposeAsync(
            t -> {
              Throwable cause = FuturesTool.unwrap(t);
              System.out.printf("Exception caught %s - %s\n", funcName, cause.getMessage());
              return CompletableFuture.failedFuture(cause);
            },
            directExecutor());
  }

  // Double-checking that exceptions thrown from both original future and transform block are being
  // caught. A checked exception can't be thrown from a java.util.function.Function, hence callAsync.
  CompletableFuture<Integer> transformCall3(CompletableFuture<Integer> future, String funcName) {
    return future
        .thenComposeAsync(
            i ->
                FuturesTool.callAsync(
                    () -> {
                      System.out.printf("Calling %s - %d\n", funcName, i);
                      if (true) throw new Exception("OOPS2");
                      return CompletableFuture.completedFuture(i);
                    }),
            directExecutor())
        .exceptionallyComposeAsync(
            t -> {
              Throwable cause = FuturesTool.unwrap(t);
              System.out.printf("Exception caught %s - %s\n", funcName, cause.getMessage());
              return CompletableFuture.failedFuture(cause);
            },
            directExecutor());
  }

  // @Test
  public void test() throws InterruptedException {
    CompletableFuture<Integer> unused1 = transformCall(success(), "success");
    CompletableFuture<Integer> unused2 = transformCall(failure(), "failure");
    Thread.sleep(10000);
  }

  // @Test
  public void test2() throws InterruptedException {
    CompletableFuture<Integer> unused1 = transformCall2(success(), "success");
    CompletableFuture<Integer> unused2 = transformCall2(failure(), "failure");
    Thread.sleep(10000);
  }

  // @Test
  public void test3() throws InterruptedException {
    CompletableFuture<Integer> unused1 = transformCall3(success(), "success");
    CompletableFuture<Integer> unused2 = transformCall3(failure(), "failure");
    Thread.sleep(10000);
  }

  // @Test
  public void test4() throws InterruptedException {
    CompletableFuture<Integer> unused =
        transformCall2(
            FuturesTool.transformAsync(
                success(),
                i -> FuturesTool.callAsync(() -> { throw new Exception("OOPS3"); }),
                directExecutor()),
            "success");
    Thread.sleep(10000);
  }

  // @Test
  public void chainTest() throws InterruptedException {
    CompletableFuture<Integer> unused =
        failure()
            .thenComposeAsync(
                i -> {
                  System.out.printf("Calling success - %d\n", i);
                  return CompletableFuture.completedFuture(i);
                },
                directExecutor())
            .thenComposeAsync(
                i -> {
                  System.out.printf("Calling success 2 - %d\n", i);
                  return CompletableFuture.completedFuture(i);
                },
                directExecutor())
            .exceptionallyComposeAsync(
                t -> {
                  Throwable cause = FuturesTool.unwrap(t);
                  System.out.printf("Exception caught 1 - %s\n", cause.getMessage());
                  return CompletableFuture.failedFuture(cause);
                },
                directExecutor())
            .thenComposeAsync(
                i -> {
                  System.out.printf("Calling success 3 - %d\n", i);
                  return CompletableFuture.completedFuture(i);
                },
                directExecutor())
            .exceptionallyComposeAsync(
                t -> {
                  System.out.printf(
                      "Exception caught 2 - %s\n", FuturesTool.unwrap(t).getMessage());
                  return CompletableFuture.completedFuture(321);
                },
                directExecutor())
            .thenComposeAsync(
                i -> {
                  System.out.printf("Calling success 4 - %d\n", i);
                  return CompletableFuture.completedFuture(i);
                },
                directExecutor())
            .exceptionallyComposeAsync(
                t -> {
                  Throwable cause = FuturesTool.unwrap(t);
                  System.out.printf("Exception caught 3 - %s\n", cause.getMessage());
                  return CompletableFuture.failedFuture(cause);
                },
                directExecutor());

    Thread.sleep(10000);
  }
}
