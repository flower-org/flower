package com.flower.engine.runner;

import java.util.concurrent.Executor;

/**
 * Runs tasks on Flower's scheduler: inline if the current thread already is a scheduler thread,
 * otherwise by submitting them to the scheduler.
 *
 * <p>Used to chain stages that call user functions (Step, Transit and Event Functions). With a
 * direct executor such stages would run on whichever thread completed the previous stage, e.g. an
 * asynchronous client's thread that completed a future returned by a Step Function. This executor
 * moves them back to Flower's scheduler instead, while avoiding a thread hop when they're already
 * there.
 *
 * <p>Scheduler threads are recognized by a thread-local mark, set the first time a thread runs a
 * task submitted by this executor, so it works with any scheduler.
 */
public final class SchedulerAffineExecutor implements Executor {
  private final Executor scheduler;
  private final ThreadLocal<Boolean> isSchedulerThread = new ThreadLocal<>();

  public SchedulerAffineExecutor(Executor scheduler) {
    this.scheduler = scheduler;
  }

  @Override
  public void execute(Runnable task) {
    if (Boolean.TRUE.equals(isSchedulerThread.get())) {
      task.run();
    } else {
      scheduler.execute(
          () -> {
            isSchedulerThread.set(Boolean.TRUE);
            task.run();
          });
    }
  }
}
