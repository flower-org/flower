package com.flower.utilities.messagepassing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

/** Plain (engine-free) tests for {@link MessageQueue}: messages paired with result futures. */
public class MessageQueueUnitTest {
    static class MyTestException extends RuntimeException {}

    static Throwable failureOf(CompletableFuture<?> future) throws Exception {
        return future.handle((v, t) -> t).get(5, TimeUnit.SECONDS);
    }

    @Test
    void newQueue_isEmpty() {
        MessageQueue<String, Integer> queue = new MessageQueue<>();
        assertTrue(queue.isEmpty());
        assertEquals(0, queue.size());
        assertNull(queue.poll());
        assertNull(queue.peek());
    }

    @Test
    void add_returnsPendingResultFuture() {
        MessageQueue<String, Integer> queue = new MessageQueue<>();
        CompletableFuture<Integer> result = queue.add("a");
        assertFalse(result.isDone());
        assertEquals(1, queue.size());
    }

    @Test
    void poll_returnsMessageWithTheProducersResultFuture() throws Exception {
        MessageQueue<String, Integer> queue = new MessageQueue<>();
        CompletableFuture<Integer> result = queue.add("a");

        Pair<String, CompletableFuture<Integer>> polled = queue.poll();
        assertNotNull(polled);
        assertEquals("a", polled.getLeft());
        assertSame(result, polled.getRight());

        polled.getRight().complete(42);
        assertEquals(42, result.get(5, TimeUnit.SECONDS));
        assertTrue(queue.isEmpty());
    }

    @Test
    void consumerFailure_isDeliveredToProducer() throws Exception {
        MessageQueue<String, Integer> queue = new MessageQueue<>();
        CompletableFuture<Integer> result = queue.add("a");
        MyTestException exception = new MyTestException();

        Pair<String, CompletableFuture<Integer>> polled = queue.poll();
        assertNotNull(polled);
        polled.getRight().completeExceptionally(exception);
        assertSame(exception, failureOf(result));
    }

    @Test
    void peek_returnsMessageWithoutRemovingIt() {
        MessageQueue<String, Integer> queue = new MessageQueue<>();
        CompletableFuture<Integer> ignored = queue.add("a");
        assertEquals("a", queue.peek());
        assertEquals(1, queue.size());
        Pair<String, CompletableFuture<Integer>> polled = queue.poll();
        assertNotNull(polled);
        assertEquals("a", polled.getLeft());
    }

    @Test
    void addAll_returnsOneFuturePerMessageInOrder() throws Exception {
        MessageQueue<String, Integer> queue = new MessageQueue<>();
        List<CompletableFuture<Integer>> results =
            new ArrayList<>(queue.addAll(List.of("a", "bb", "ccc")));
        assertEquals(3, results.size());
        assertEquals(3, queue.size());

        Pair<String, CompletableFuture<Integer>> polled;
        int index = 0;
        while ((polled = queue.poll()) != null) {
            assertSame(results.get(index++), polled.getRight());
            polled.getRight().complete(polled.getLeft().length());
        }
        assertEquals(1, results.get(0).get(5, TimeUnit.SECONDS));
        assertEquals(2, results.get(1).get(5, TimeUnit.SECONDS));
        assertEquals(3, results.get(2).get(5, TimeUnit.SECONDS));
    }

    @Test
    void addAll_empty_returnsNoFuturesAndDoesNotNotify() {
        MessageQueue<String, Integer> queue = new MessageQueue<>();
        CompletableFuture<Void> listener = queue.getMessageListener();
        Collection<CompletableFuture<Integer>> results = queue.addAll(List.of());
        assertTrue(results.isEmpty());
        assertFalse(listener.isDone());
    }

    @Test
    void messageListener_completesOnAddAndAddAll() {
        MessageQueue<String, Integer> queue = new MessageQueue<>();
        CompletableFuture<Void> listener1 = queue.getMessageListener();
        CompletableFuture<Integer> ignored = queue.add("a");
        assertTrue(listener1.isDone());

        Pair<String, CompletableFuture<Integer>> drained = queue.poll();
        assertNotNull(drained);
        CompletableFuture<Void> listener2 = queue.getMessageListener();
        assertFalse(listener2.isDone());
        Collection<CompletableFuture<Integer>> unusedResults = queue.addAll(List.of("b", "c"));
        assertTrue(listener2.isDone());
    }

    @Test
    void messageListener_nonEmptyQueue_completesImmediately() {
        MessageQueue<String, Integer> queue = new MessageQueue<>();
        CompletableFuture<Integer> ignored = queue.add("a");
        assertTrue(queue.getMessageListener().isDone());
    }

    @Test
    void notifyMessageListeners_viaSink_forcesWakeUp() {
        MessageQueue<String, Integer> queue = new MessageQueue<>();
        MessageSink<String, Integer> sink = queue;
        MessageSource<String, Integer> source = queue;
        CompletableFuture<Void> listener = source.getMessageListener();
        sink.notifyMessageListeners();
        assertTrue(listener.isDone());
        assertNull(source.poll());
    }

    /**
     * Engine-free version of the flow-based MessageQueueTest: listener-driven consumers drain the
     * queue and complete each message's future with the message itself; every producer future must
     * complete with its own message.
     */
    @Test
    void concurrentProducersAndConsumers_everyResultFutureCompletedWithItsMessage() throws Exception {
        final int producers = 4;
        final int messagesPerProducer = 2_500;
        final int total = producers * messagesPerProducer;
        final int consumers = 2;

        MessageQueue<Integer, Integer> queue = new MessageQueue<>();
        MessageSink<Integer, Integer> sink = queue;
        MessageSource<Integer, Integer> source = queue;
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(consumers);
        ExecutorService producerExecutor = Executors.newFixedThreadPool(producers);
        try {
            AtomicInteger processed = new AtomicInteger();
            AtomicInteger[] processedBy = new AtomicInteger[consumers];

            for (int c = 0; c < consumers; c++) {
                processedBy[c] = new AtomicInteger();
                AtomicInteger mine = processedBy[c];
                Runnable[] drainAndRearm = new Runnable[1];
                drainAndRearm[0] = () -> {
                    Pair<Integer, CompletableFuture<Integer>> message;
                    while ((message = source.poll()) != null) {
                        message.getRight().complete(message.getLeft());
                        processed.incrementAndGet();
                        mine.incrementAndGet();
                    }
                    if (processed.get() < total) {
                        CompletableFuture<?> ignored =
                            source.getMessageListener().thenRunAsync(drainAndRearm[0], consumerExecutor);
                    }
                };
                consumerExecutor.execute(drainAndRearm[0]);
            }

            List<CompletableFuture<List<CompletableFuture<Integer>>>> producerResults = new ArrayList<>();
            for (int p = 0; p < producers; p++) {
                int base = p * messagesPerProducer;
                producerResults.add(CompletableFuture.supplyAsync(() -> {
                    List<CompletableFuture<Integer>> futures = new ArrayList<>();
                    for (int i = 0; i < messagesPerProducer; i++) {
                        futures.add(sink.add(base + i));
                    }
                    return futures;
                }, producerExecutor));
            }

            for (int p = 0; p < producers; p++) {
                List<CompletableFuture<Integer>> futures = producerResults.get(p).get(30, TimeUnit.SECONDS);
                int base = p * messagesPerProducer;
                for (int i = 0; i < futures.size(); i++) {
                    assertEquals(base + i, futures.get(i).get(30, TimeUnit.SECONDS));
                }
            }

            assertEquals(total, processed.get());
            int sum = 0;
            for (AtomicInteger count : processedBy) {
                sum += count.get();
            }
            assertEquals(total, sum);
            assertTrue(queue.isEmpty());
        } finally {
            producerExecutor.shutdownNow();
            consumerExecutor.shutdownNow();
        }
    }
}
