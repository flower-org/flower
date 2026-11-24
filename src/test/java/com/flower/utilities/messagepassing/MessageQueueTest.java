package com.flower.utilities.messagepassing;

import java.util.concurrent.CompletableFuture;
import com.flower.utilities.FuturesTool;

import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.State;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.params.common.In;
import com.flower.anno.params.common.InOut;
import com.flower.anno.params.transit.StepRef;
import com.flower.anno.params.transit.Terminal;
import com.flower.conf.FlowExec;
import com.flower.conf.FlowFuture;
import com.flower.conf.InOutPrm;
import com.flower.conf.Transition;
import com.flower.engine.Flower;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MessageQueueTest {
    static final int MESSAGE_COUNT = 10_000;

    @Test
    public void test() throws InterruptedException, ExecutionException {
        MessageQueue<Integer, Integer> messageQueue = new MessageQueue<>();

        Flower flower = new Flower(2);
        flower.registerFlow(ProcessorFlow.class);
        flower.initialize();

        FlowExec<ProcessorFlow> flowExec = flower.getFlowExec(ProcessorFlow.class);
        AtomicInteger processedCount = new AtomicInteger(0);

        ProcessorFlow testFlow = new ProcessorFlow(messageQueue, processedCount, 1);
        FlowFuture<ProcessorFlow> flowFuture1 = flowExec.runFlow(testFlow);
        System.out.println("Flow created. Id: " + flowFuture1.getFlowId());
        ProcessorFlow testFlow2 = new ProcessorFlow(messageQueue, processedCount, 2);
        FlowFuture<ProcessorFlow> flowFuture2 = flowExec.runFlow(testFlow2);
        System.out.println("Flow created. Id: " + flowFuture2.getFlowId());

        // We use MessageSink here to hide all non-Producer related functions
        MessageSink<Integer, Integer> messageSink = messageQueue;
        List<CompletableFuture<Integer>> list = new ArrayList<>();
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            System.out.println("Sending " + i);
            CompletableFuture<Integer> future = messageSink.add(i);
            list.add(future);
            if (i % 1000 == 0) {
                Thread.sleep(10);
            }
        }

        CompletableFuture<Void> lock = new CompletableFuture<>();
        CompletableFuture<Boolean> unused =
            FuturesTool.whenAllCompleteCall(list, () -> lock.complete(null), FuturesTool.directExecutor());
        lock.get();

        while (!flowFuture1.getFuture().isDone() ||
                !flowFuture2.getFuture().isDone()) {
          System.out.println("Forcing wake-up to finalize flows");
          messageSink.notifyMessageListeners();
          Thread.sleep(10);
        }

        System.out.println("IsDone 1 " + flowFuture1.getFuture().isDone());
        System.out.println("IsDone 2 " + flowFuture2.getFuture().isDone());

        ProcessorFlow processorFlow1 = flowFuture1.getFuture().get();
        ProcessorFlow processorFlow2 = flowFuture2.getFuture().get();
        assertEquals(MESSAGE_COUNT, processorFlow1.processedByMe + processorFlow2.processedByMe);

        assertEquals(MESSAGE_COUNT, processedCount.get());
        for (int i = 0; i < list.size(); i++) {
            CompletableFuture<Integer> future = list.get(i);
            int result = future.get();
            assertEquals(result, i);
        }
    }

    @FlowType(firstStep = "PROCESS_MESSAGE")
    static class ProcessorFlow {
        @State final MessageSource<Integer, Integer> messageSource;
        @State final AtomicInteger processedCount;
        @State final int flowNumber;
        @State int processedByMe;

        public ProcessorFlow(MessageSource<Integer, Integer> messageSource,
                             AtomicInteger processedCount,
                             int flowNumber) {
          this.messageSource = messageSource;
          this.processedCount = processedCount;
          this.flowNumber = flowNumber;
        }

        @SimpleStepFunction
        public static CompletableFuture<Transition> PROCESS_MESSAGE(@In MessageSource<Integer, Integer> messageSource,
                                                 @In AtomicInteger processedCount,
                                                 @In int flowNumber,
                                                 @InOut(throwIfNull = true) InOutPrm<Integer> processedByMe,
                                                 @StepRef Transition PROCESS_MESSAGE,
                                                 @Terminal Transition END) {
            int processedByMeVal = processedByMe.getInValue();

            System.out.println(flowNumber + " Flow woke up. Processed: " + processedCount.get() + "; By me: " + processedByMeVal);

            Pair<Integer, CompletableFuture<Integer>> message = messageSource.poll();
            while (message != null) {
                // We must add this null check because the queue is concurrent and race conditions are possible
                int i = message.getKey();
                System.out.println(flowNumber + " Processing " + i);
                message.getRight().complete(i);
                processedCount.incrementAndGet();
                processedByMeVal++;

                message = messageSource.poll();
            }
            processedByMe.setOutValue(processedByMeVal);

            if (processedCount.get() >= MESSAGE_COUNT) {
                System.out.println(flowNumber + " Flow DONE. Processed: " + processedCount.get() + "; By me: " + processedByMeVal);
                return CompletableFuture.completedFuture(END);
            } else {
                System.out.println(flowNumber + " Flow Gon'sleep. Processed: " + processedCount.get() + "; By me: " + processedByMeVal);
                return FuturesTool.transform(messageSource.getMessageListener(),
                    unused_ -> PROCESS_MESSAGE,
                    FuturesTool.directExecutor()
                );
            }
        }
    }
}
