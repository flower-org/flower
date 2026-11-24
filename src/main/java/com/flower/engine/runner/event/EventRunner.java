package com.flower.engine.runner.event;

import com.flower.anno.event.EventType;
import com.flower.conf.Transition;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

public interface EventRunner {
  CompletableFuture<Void> runEvents(
      EventType eventType,
      EventContext eventContext,
      EventParametersProvider eventParametersProvider,
      @Nullable Transition transition,
      @Nullable Throwable flowException);
}
