package com.flower.utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * CompletableFuture helpers. Semantics mirror the Guava {@code Futures} helpers flower was
 * originally built on:
 *
 * <ul>
 *   <li>Failures are delivered unwrapped: {@link CompletionException} wrappers added by
 *       CompletableFuture are stripped, so callbacks and downstream futures see the original
 *       exception.
 *   <li>Functions passed together with an Executor always run on that Executor.
 *   <li>If the Executor rejects a task, the returned future fails with the rejection exception
 *       (plain CompletableFuture {@code *Async} methods would leave the dependent future incomplete).
 *   <li>Cancelling a returned future does not cancel its inputs.
 * </ul>
 */
public class FuturesTool {

  private enum DirectExecutor implements Executor {
    INSTANCE;

    @Override
    public void execute(Runnable command) {
      command.run();
    }

    @Override
    public String toString() {
      return "FuturesTool.directExecutor()";
    }
  }

  /** Executor that runs tasks immediately in the calling thread. */
  public static Executor directExecutor() {
    return DirectExecutor.INSTANCE;
  }

  /**
   * Strips {@link CompletionException} wrappers that CompletableFuture adds when propagating a
   * failure to dependent stages.
   */
  public static Throwable unwrap(Throwable t) {
    Throwable current = t;
    while (current instanceof CompletionException && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  /**
   * Runs {@code task} on {@code executor}; if the executor rejects it, fails {@code result} with
   * the rejection exception.
   */
  private static void executeOrFail(
      Executor executor, CompletableFuture<?> result, Runnable task) {
    try {
      executor.execute(task);
    } catch (Throwable t) {
      result.completeExceptionally(t);
    }
  }

  /** Completes {@code target} with the outcome of {@code source}, unwrapping failures. */
  private static <V> void propagate(CompletableFuture<? extends V> source, CompletableFuture<V> target) {
    CompletableFuture<?> ignored =
        source.whenComplete(
        (v, t) -> {
          if (t == null) {
            target.complete(v);
          } else {
            target.completeExceptionally(unwrap(t));
          }
        });
  }

  /** Listens to {@code future} and runs {@code callback} on {@code executor} once it completes. */
  private static <V> void onComplete(
      CompletableFuture<? extends V> future,
      CompletableFuture<?> result,
      BiConsumer<? super V, ? super Throwable> callback,
      Executor executor) {
    CompletableFuture<?> ignored =
        future.whenComplete(
        (v, t) -> executeOrFail(executor, result, () -> callback.accept(v, t == null ? null : unwrap(t))));
  }

  /** Equivalent of Guava {@code Futures.transform(input, function, executor)}. */
  public static <I, O> CompletableFuture<O> transform(
      CompletableFuture<I> input, Function<? super I, ? extends O> function, Executor executor) {
    CompletableFuture<O> result = new CompletableFuture<>();
    onComplete(
        input,
        result,
        (v, t) -> {
          if (t != null) {
            result.completeExceptionally(t);
            return;
          }
          try {
            result.complete(function.apply(v));
          } catch (Throwable e) {
            result.completeExceptionally(e);
          }
        },
        executor);
    return result;
  }

  /** Equivalent of Guava {@code Futures.transformAsync(input, function, executor)}. */
  public static <I, O> CompletableFuture<O> transformAsync(
      CompletableFuture<I> input,
      Function<? super I, ? extends CompletableFuture<? extends O>> function,
      Executor executor) {
    CompletableFuture<O> result = new CompletableFuture<>();
    onComplete(
        input,
        result,
        (v, t) -> {
          if (t != null) {
            result.completeExceptionally(t);
            return;
          }
          applyAsyncFunction(function, v, result);
        },
        executor);
    return result;
  }

  /** Equivalent of Guava {@code Futures.catching(input, exceptionType, fallback, executor)}. */
  public static <V, X extends Throwable> CompletableFuture<V> catching(
      CompletableFuture<? extends V> input,
      Class<X> exceptionType,
      Function<? super X, ? extends V> fallback,
      Executor executor) {
    CompletableFuture<V> result = new CompletableFuture<>();
    onComplete(
        input,
        result,
        (v, t) -> {
          if (t == null) {
            result.complete(v);
          } else if (!exceptionType.isInstance(t)) {
            result.completeExceptionally(t);
          } else {
            try {
              result.complete(fallback.apply(exceptionType.cast(t)));
            } catch (Throwable e) {
              result.completeExceptionally(e);
            }
          }
        },
        executor);
    return result;
  }

  /** Equivalent of Guava {@code Futures.catchingAsync(input, exceptionType, fallback, executor)}. */
  public static <V, X extends Throwable> CompletableFuture<V> catchingAsync(
      CompletableFuture<? extends V> input,
      Class<X> exceptionType,
      Function<? super X, ? extends CompletableFuture<? extends V>> fallback,
      Executor executor) {
    CompletableFuture<V> result = new CompletableFuture<>();
    onComplete(
        input,
        result,
        (v, t) -> {
          if (t == null) {
            result.complete(v);
          } else if (!exceptionType.isInstance(t)) {
            result.completeExceptionally(t);
          } else {
            applyAsyncFunction(fallback, exceptionType.cast(t), result);
          }
        },
        executor);
    return result;
  }

  private static <I, O> void applyAsyncFunction(
      Function<? super I, ? extends CompletableFuture<? extends O>> function,
      I input,
      CompletableFuture<O> result) {
    CompletableFuture<? extends O> output;
    try {
      output = function.apply(input);
    } catch (Throwable e) {
      result.completeExceptionally(e);
      return;
    }
    if (output == null) {
      result.completeExceptionally(
          new NullPointerException(
              "Async function returned null instead of a CompletableFuture: " + function));
    } else {
      propagate(output, result);
    }
  }

  /**
   * Returns a future that completes (successfully, with null) once all {@code futures} are done,
   * regardless of whether they succeeded. Unlike {@link CompletableFuture#allOf}, never fails.
   */
  public static CompletableFuture<Void> whenAllComplete(
      Iterable<? extends CompletableFuture<?>> futures) {
    List<CompletableFuture<?>> handled = new ArrayList<>();
    for (CompletableFuture<?> future : futures) {
      handled.add(future.handle((v, t) -> null));
    }
    return CompletableFuture.allOf(handled.toArray(new CompletableFuture<?>[0]));
  }

  /** See {@link #whenAllComplete(Iterable)}. */
  public static CompletableFuture<Void> whenAllComplete(CompletableFuture<?>... futures) {
    return whenAllComplete(List.of(futures));
  }

  /**
   * Equivalent of Guava {@code Futures.whenAllComplete(futures).call(combiner, executor)}: once all
   * {@code futures} are done (successfully or not), runs {@code combiner} on {@code executor}.
   */
  public static <V> CompletableFuture<V> whenAllCompleteCall(
      Iterable<? extends CompletableFuture<?>> futures, Callable<V> combiner, Executor executor) {
    CompletableFuture<V> result = new CompletableFuture<>();
    onComplete(
        whenAllComplete(futures),
        result,
        (ignored, t) -> {
          try {
            result.complete(combiner.call());
          } catch (Throwable e) {
            result.completeExceptionally(e);
          }
        },
        executor);
    return result;
  }

  /**
   * Equivalent of Guava {@code Futures.whenAllComplete(futures).run(combiner, executor)}: once all
   * {@code futures} are done (successfully or not), runs {@code combiner} on {@code executor}.
   */
  public static CompletableFuture<Void> whenAllCompleteRun(
      Iterable<? extends CompletableFuture<?>> futures, Runnable combiner, Executor executor) {
    return transform(
        whenAllComplete(futures),
        ignored -> {
          combiner.run();
          return null;
        },
        executor);
  }

  public static <V> CompletableFuture<V> whenAnyComplete(
      Iterable<? extends CompletableFuture<? extends V>> futures, Executor executor) {
    CompletableFuture<V> retFuture = new CompletableFuture<>();
    AtomicBoolean doneFlag = new AtomicBoolean(false);
    for (CompletableFuture<? extends V> future : futures) {
      onComplete(
          future,
          retFuture,
          (v, t) -> {
            if (false == doneFlag.compareAndExchange(false, true)) {
              if (t == null) {
                retFuture.complete(v);
              } else {
                retFuture.completeExceptionally(t);
              }
            }
          },
          executor);
    }

    return retFuture;
  }

  public static CompletableFuture<Void> whenAnyCompleteIgnoreResult(
      Iterable<? extends CompletableFuture<?>> futures, Executor executor) {
    CompletableFuture<Void> retFuture = new CompletableFuture<>();
    AtomicBoolean doneFlag = new AtomicBoolean(false);
    for (CompletableFuture<?> future : futures) {
      onComplete(
          future,
          retFuture,
          (v, t) -> {
            if (false == doneFlag.compareAndExchange(false, true)) {
              retFuture.complete(null);
            }
          },
          executor);
    }

    return retFuture;
  }

  public static <I, O, X extends Throwable> CompletableFuture<O> tryCatch(
      CompletableFuture<I> input,
      Function<? super I, ? extends O> function,
      Class<X> exceptionType,
      Function<? super X, ? extends O> fallback,
      Executor executor) {
    return catching(transform(input, function, executor), exceptionType, fallback, executor);
  }

  public static <I, O, X extends Throwable> CompletableFuture<O> tryCatchAsync(
      CompletableFuture<I> input,
      Function<? super I, ? extends CompletableFuture<? extends O>> function,
      Class<X> exceptionType,
      Function<? super X, ? extends CompletableFuture<? extends O>> fallback,
      Executor executor) {
    return catchingAsync(
        transformAsync(input, function, executor), exceptionType, fallback, executor);
  }

  /**
   * Completes {@code setAndReturnThisFuture} with the outcome of {@code futureToDecouple}, from a
   * task submitted to {@code executor}. Decouples the two futures so that chaining steps this way
   * does not build an ever-growing chain of dependent futures.
   */
  public static <V> void assignSettableFuture(
      CompletableFuture<V> futureToDecouple,
      CompletableFuture<V> setAndReturnThisFuture,
      Executor executor) {
    onComplete(
        futureToDecouple,
        setAndReturnThisFuture,
        (v, t) -> {
          if (t == null) {
            setAndReturnThisFuture.complete(v);
          } else {
            setAndReturnThisFuture.completeExceptionally(t);
          }
        },
        executor);
  }
}
