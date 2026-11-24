package com.flower.parameters;

import java.util.concurrent.ScheduledExecutorService;

import com.flower.anno.event.Concurrency;
import com.flower.anno.event.EventFunction;
import com.flower.anno.event.EventProfileContainer;
import com.flower.anno.event.EventProfiles;
import com.flower.anno.event.EventType;
import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.GlobalFunctionContainer;
import com.flower.anno.flow.State;
import com.flower.anno.functions.GlobalFunction;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.functions.StepCall;
import com.flower.anno.functions.StepFunction;
import com.flower.anno.functions.TransitFunction;
import com.flower.anno.params.common.Exec;
import com.flower.anno.params.common.Out;
import com.flower.anno.params.events.EventInfo;
import com.flower.anno.params.transit.Terminal;
import com.flower.conf.FlowFuture;
import com.flower.conf.OutPrm;
import com.flower.conf.Transition;
import com.flower.engine.Flower;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExecutorParameterTest {
    @Test
    public void test1() throws ExecutionException, InterruptedException {
        Flower flower = new Flower();
        flower.registerFlow(P_TestExecutorFlow.class);
        flower.initialize();

        FlowFuture<P_TestExecutorFlow> execFlow = flower.getFlowExec(P_TestExecutorFlow.class).runFlow(new P_TestExecutorFlow());
        assertTrue(execFlow.getFuture().get().executor instanceof ScheduledExecutorService);
    }

    @Test
    public void execInEventFunction_followedByOtherParameter() throws Exception {
        P_ExecEventProfile.executor.set(null);
        P_ExecEventProfile.eventType.set(null);
        Flower flower = new Flower();
        flower.registerFlow(P_ExecEventFlow.class);
        flower.registerEventProfile(P_ExecEventProfile.class);
        flower.initialize();
        try {
            // FINALIZER, so a failing call fails the flow instead of being swallowed
            flower.getFlowExec(P_ExecEventFlow.class).runFlow(new P_ExecEventFlow()).getFuture()
                .get(10, TimeUnit.SECONDS);
            assertTrue(P_ExecEventProfile.executor.get() instanceof ScheduledExecutorService);
            assertEquals(EventType.BEFORE_FLOW, P_ExecEventProfile.eventType.get());
        } finally {
            flower.shutdownScheduler();
        }
    }

    @Test
    public void execInStepCallToGlobalFunction() throws Exception {
        Flower flower = new Flower();
        flower.registerGlobalFunctions(P_ExecGlobals.class);
        flower.registerFlow(P_ExecCallFlow.class);
        flower.initialize();
        try {
            P_ExecCallFlow result = flower.getFlowExec(P_ExecCallFlow.class)
                .runFlow(new P_ExecCallFlow()).getFuture().get(10, TimeUnit.SECONDS);
            assertTrue(result.executor instanceof ScheduledExecutorService);
        } finally {
            flower.shutdownScheduler();
        }
    }

    @Test
    public void execWrongType_rejected() {
        Flower flower = new Flower();
        try {
            Throwable e = assertThrows(Throwable.class, () -> {
                flower.registerFlow(P_ExecWrongTypeFlow.class);
                flower.initialize();
            });
            assertTrue(
                e.getMessage().contains(
                    "annotated as @Exec should be of type " + Executor.class.getCanonicalName()),
                e.getMessage());
        } finally {
            flower.shutdownScheduler();
        }
    }
}

@FlowType(firstStep = "step")
class P_TestExecutorFlow {
    @State Executor executor;

    @StepFunction(transit = "transit")
    static void step(@Exec Executor flowerExecutor,
                     @Out OutPrm<Executor> executor) {
        executor.setOutValue(flowerExecutor);
    }

    @TransitFunction
    static Transition transit(@Terminal Transition end) {
        return end;
    }
}

@FlowType(firstStep = "step")
@EventProfiles({P_ExecEventProfile.class})
class P_ExecEventFlow {
    @SimpleStepFunction
    static Transition step(@Terminal Transition end) {
        return end;
    }
}

@EventProfileContainer(name = "P_ExecEventProfile")
class P_ExecEventProfile {
    static final AtomicReference<Executor> executor = new AtomicReference<>();
    static final AtomicReference<EventType> eventType = new AtomicReference<>();

    public P_ExecEventProfile() {}

    @EventFunction(types = EventType.BEFORE_FLOW, concurrency = Concurrency.FINALIZER)
    static void event(@Exec Executor flowerExecutor, @EventInfo EventType event) {
        executor.set(flowerExecutor);
        eventType.set(event);
    }
}

@GlobalFunctionContainer
class P_ExecGlobals {
    @GlobalFunction
    static void capture(@Exec Executor flowerExecutor, @Out OutPrm<Executor> executor) {
        executor.setOutValue(flowerExecutor);
    }
}

@FlowType(firstStep = "step")
class P_ExecCallFlow {
    @State Executor executor;

    @StepCall(globalFunctionContainer = P_ExecGlobals.class, globalFunctionName = "capture", transit = "transit")
    static void step(@Exec Executor flowerExecutor, @Out OutPrm<Executor> executor) {}

    @TransitFunction
    static Transition transit(@Terminal Transition end) {
        return end;
    }
}

@FlowType(firstStep = "step")
class P_ExecWrongTypeFlow {
    @SimpleStepFunction
    static Transition step(@Exec Object flowerExecutor, @Terminal Transition end) {
        return end;
    }
}

//TODO: test-cover other call contexts: Transit, etc