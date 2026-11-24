package com.flower.utilities.messagepassing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Plain (engine-free) tests for {@link SimpleMessageQueue}, covering the shared {@link
 * AbstractMessageQueue} behaviour: listeners, notification and ordering.
 */
public class SimpleMessageQueueUnitTest {
    // ---------------- basic queue operations

    @Test
    void newQueue_isEmpty() {
        SimpleMessageQueue<Integer> queue = new SimpleMessageQueue<>();
        assertTrue(queue.isEmpty());
        assertEquals(0, queue.size());
        assertNull(queue.poll());
        assertNull(queue.peek());
    }

    @Test
    void add_pollAndPeek_fifoOrder() {
        SimpleMessageQueue<Integer> queue = new SimpleMessageQueue<>();
        queue.add(1);
        queue.add(2);
        queue.addAll(List.of(3, 4));

        assertFalse(queue.isEmpty());
        assertEquals(4, queue.size());
        assertEquals(1, queue.peek());
        assertEquals(4, queue.size());

        List<Integer> polled = new ArrayList<>();
        Integer message;
        while ((message = queue.poll()) != null) {
            polled.add(message);
        }
        assertEquals(List.of(1, 2, 3, 4), polled);
        assertTrue(queue.isEmpty());
    }

    // ---------------- message listeners

    @Test
    void messageListener_emptyQueue_staysPending() {
        SimpleMessageQueue<Integer> queue = new SimpleMessageQueue<>();
        assertFalse(queue.getMessageListener().isDone());
    }

    @Test
    void messageListener_completesOnAdd() throws Exception {
        SimpleMessageQueue<Integer> queue = new SimpleMessageQueue<>();
        CompletableFuture<Void> listener = queue.getMessageListener();
        queue.add(1);
        assertTrue(listener.isDone());
        assertNull(listener.get());
    }

    @Test
    void messageListener_completesOnAddAll() {
        SimpleMessageQueue<Integer> queue = new SimpleMessageQueue<>();
        CompletableFuture<Void> listener = queue.getMessageListener();
        queue.addAll(List.of(1, 2));
        assertTrue(listener.isDone());
    }

    @Test
    void messageListener_addAllEmpty_doesNotNotify() {
        SimpleMessageQueue<Integer> queue = new SimpleMessageQueue<>();
        CompletableFuture<Void> listener = queue.getMessageListener();
        queue.addAll(List.of());
        assertFalse(listener.isDone());
    }

    /** Registering while messages are already queued must not miss them (no lost wake-up). */
    @Test
    void messageListener_nonEmptyQueue_completesImmediately() {
        SimpleMessageQueue<Integer> queue = new SimpleMessageQueue<>();
        queue.add(1);
        assertTrue(queue.getMessageListener().isDone());
    }

    @Test
    void messageListener_allListenersNotified() {
        SimpleMessageQueue<Integer> queue = new SimpleMessageQueue<>();
        CompletableFuture<Void> listener1 = queue.getMessageListener();
        CompletableFuture<Void> listener2 = queue.getMessageListener();
        CompletableFuture<Void> listener3 = queue.getMessageListener();
        queue.add(1);
        assertTrue(listener1.isDone());
        assertTrue(listener2.isDone());
        assertTrue(listener3.isDone());
    }

    @Test
    void messageListener_isOneShot_newListenerWaitsForNextMessage() {
        SimpleMessageQueue<Integer> queue = new SimpleMessageQueue<>();
        CompletableFuture<Void> first = queue.getMessageListener();
        queue.add(1);
        assertTrue(first.isDone());
        assertEquals(1, queue.poll());

        CompletableFuture<Void> second = queue.getMessageListener();
        assertNotSame(first, second);
        assertFalse(second.isDone());
        queue.add(2);
        assertTrue(second.isDone());
    }

    @Test
    void notifyMessageListeners_emptyQueue_forcesWakeUp() {
        SimpleMessageQueue<Integer> queue = new SimpleMessageQueue<>();
        CompletableFuture<Void> listener = queue.getMessageListener();
        queue.notifyMessageListeners();
        assertTrue(listener.isDone());
        assertTrue(queue.isEmpty());
    }

    @Test
    void notifyMessageListenersNotForced_emptyQueue_doesNotWake() {
        SimpleMessageQueue<Integer> queue = new SimpleMessageQueue<>();
        CompletableFuture<Void> listener = queue.getMessageListener();
        queue.notifyMessageListeners(false);
        assertFalse(listener.isDone());
    }

    @Test
    void notifyMessageListeners_noListeners_isNoOp() {
        SimpleMessageQueue<Integer> queue = new SimpleMessageQueue<>();
        queue.notifyMessageListeners();
        queue.notifyMessageListeners(false);
        // An earlier notification is not remembered: a listener registered afterwards still waits.
        assertFalse(queue.getMessageListener().isDone());
    }

    @Test
    void messageListener_cancelledByConsumer_doesNotBreakOtherListeners() {
        SimpleMessageQueue<Integer> queue = new SimpleMessageQueue<>();
        CompletableFuture<Void> cancelled = queue.getMessageListener();
        CompletableFuture<Void> other = queue.getMessageListener();
        cancelled.cancel(false);
        queue.add(1);
        assertTrue(cancelled.isCancelled());
        assertTrue(other.isDone());
        assertFalse(other.isCompletedExceptionally());
    }

    @Test
    void sinkAndSourceViews_shareTheQueue() {
        SimpleMessageQueue<Integer> queue = new SimpleMessageQueue<>();
        SimpleMessageSink<Integer> sink = queue;
        SimpleMessageSource<Integer> source = queue;

        CompletableFuture<Void> listener = source.getMessageListener();
        sink.add(7);
        assertTrue(listener.isDone());
        assertEquals(7, source.peek());
        assertEquals(7, source.poll());
        assertNull(source.poll());
    }

    // ---------------- concurrency

    /**
     * Non-blocking consumers that, like flows, drain the queue and then re-arm a listener, while
     * several producers add concurrently: every message is consumed exactly once and no consumer is
     * left asleep while messages are queued.
     */
    @Test
    void concurrentProducersAndListenerDrivenConsumers_deliverEveryMessageOnce() throws Exception {
        final int producers = 4;
        final int messagesPerProducer = 5_000;
        final int total = producers * messagesPerProducer;
        final int consumers = 3;

        SimpleMessageQueue<Integer> queue = new SimpleMessageQueue<>();
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(consumers);
        ExecutorService producerExecutor = Executors.newFixedThreadPool(producers);
        try {
            ConcurrentHashMap<Integer, Boolean> seen = new ConcurrentHashMap<>();
            AtomicInteger duplicates = new AtomicInteger();
            AtomicInteger consumed = new AtomicInteger();
            CountDownLatch allConsumed = new CountDownLatch(1);

            Runnable[] drainAndRearm = new Runnable[1];
            drainAndRearm[0] = () -> {
                Integer message;
                while ((message = queue.poll()) != null) {
                    if (seen.putIfAbsent(message, true) != null) {
                        duplicates.incrementAndGet();
                    }
                    if (consumed.incrementAndGet() == total) {
                        allConsumed.countDown();
                    }
                }
                if (consumed.get() < total) {
                    CompletableFuture<?> ignored =
                        queue.getMessageListener().thenRunAsync(drainAndRearm[0], consumerExecutor);
                }
            };
            for (int c = 0; c < consumers; c++) {
                consumerExecutor.execute(drainAndRearm[0]);
            }

            for (int p = 0; p < producers; p++) {
                int base = p * messagesPerProducer;
                producerExecutor.execute(() -> {
                    for (int i = 0; i < messagesPerProducer; i++) {
                        queue.add(base + i);
                    }
                });
            }

            assertTrue(allConsumed.await(30, TimeUnit.SECONDS),
                "consumed only " + consumed.get() + " of " + total);
            assertEquals(0, duplicates.get());
            assertEquals(total, seen.size());
            assertTrue(queue.isEmpty());
        } finally {
            producerExecutor.shutdownNow();
            consumerExecutor.shutdownNow();
        }
    }
}
