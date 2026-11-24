package com.flower.engine.function;

import com.flower.conf.OutPrm;
import java.util.concurrent.CompletableFuture;
import java.util.Optional;

public class FlowerOutPrm<T> implements OutPrm<T> {
  Optional<T> value = Optional.empty();
  Optional<CompletableFuture<T>> valueFuture = Optional.empty();

  @Override
  public void setOutValue(T value) {
    this.value = Optional.ofNullable(value);
    this.valueFuture = Optional.empty();
  }

  @Override
  public void setOutFuture(CompletableFuture<T> valueFuture) {
    this.value = Optional.empty();
    this.valueFuture = Optional.ofNullable(valueFuture);
  }

  public Optional<T> getOpt() {
    return value;
  }

  public Optional<CompletableFuture<T>> getOptFuture() {
    return valueFuture;
  }
}
