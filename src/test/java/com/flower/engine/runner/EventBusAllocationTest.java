package com.flower.engine.runner;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.flower.anno.flow.FlowType;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.functions.StepFunction;
import com.flower.anno.functions.TransitFunction;
import com.flower.anno.params.common.EventBus;
import com.flower.anno.params.transit.StepRef;
import com.flower.anno.params.transit.Terminal;
import com.flower.conf.Transition;
import com.flower.engine.FlowImpl;
import com.flower.engine.Flower;
import com.flower.engine.FlowerId;
import com.flower.eventbus.FlowEventBus;
import org.junit.jupiter.api.Test;

/** FlowEventBus is only allocated for Flows whose type has an @EventBus parameter. */
public class EventBusAllocationTest {
  static FlowCallContext flowCallContext(Flower flower, Class<?> flowType) {
    return ((FlowExecImpl<?>) flower.getFlowExec(flowType)).flowCallContext;
  }

  @Test
  void noEventBusParameter_noBus() throws Exception {
    Flower flower = new Flower();
    try {
      flower.registerFlow(EBA_NoBusFlow.class);
      flower.initialize();
      FlowCallContext context = flowCallContext(flower, EBA_NoBusFlow.class);

      assertFalse(context.usesEventBus);
      FlowImpl<EBA_NoBusFlow> flow = context.createFlow(new FlowerId(), new EBA_NoBusFlow());
      assertNull(flow.getEventBus());
    } finally {
      flower.shutdownScheduler();
    }
  }

  @Test
  void eventBusInLaterTransit_busPerFlow() throws Exception {
    Flower flower = new Flower();
    try {
      flower.registerFlow(EBA_BusInTransitFlow.class);
      flower.initialize();
      FlowCallContext context = flowCallContext(flower, EBA_BusInTransitFlow.class);

      assertTrue(context.usesEventBus);
      FlowEventBus bus1 =
          context.createFlow(new FlowerId(), new EBA_BusInTransitFlow()).getEventBus();
      FlowEventBus bus2 =
          context.createFlow(new FlowerId(), new EBA_BusInTransitFlow()).getEventBus();
      assertNotNull(bus1);
      assertNotNull(bus2);
      assertNotSame(bus1, bus2);
    } finally {
      flower.shutdownScheduler();
    }
  }
}

@FlowType(name = "Test", firstStep = "step")
class EBA_NoBusFlow {
  @SimpleStepFunction
  static Transition step(@Terminal Transition end) {
    return end;
  }
}

@FlowType(name = "Test", firstStep = "step1")
class EBA_BusInTransitFlow {
  @SimpleStepFunction
  static Transition step1(@StepRef Transition step2) {
    return step2;
  }

  @StepFunction(transit = "transit")
  static void step2() {}

  @TransitFunction
  static Transition transit(@EventBus FlowEventBus bus, @Terminal Transition end) {
    return end;
  }
}
