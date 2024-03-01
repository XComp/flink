/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util.concurrent;

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.flink.shaded.guava31.com.google.common.base.Predicates;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;

class FutureUtilsRetryTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService>
            SCHEDULED_EXECUTOR_EXTENSION = TestingUtils.defaultExecutorExtension();

    private static final String FINAL_RESULT = "correct-value";
    private static final Throwable FINAL_EXCEPTION =
            new RuntimeException("Expected final exception.");
    private static final String INTERMEDIATE_RESULT_CAUSING_RETRY =
            "intermediate-result-causing-retry";

    private static final Throwable EXCEPTION_CAUSING_RETRY =
            new RuntimeException("Exception causing retry");
    private final AtomicInteger callCount = new AtomicInteger();

    @BeforeEach
    void initialize() {
        callCount.set(0);
    }

    /* ********************************************
     * test utility methods
     */

    private RetryStrategy retries(int maxNumberOfRetries) {
        return new FixedRetryStrategy(maxNumberOfRetries, Duration.ZERO);
    }

    private RetryStrategy retriesForever() {
        return retries(Integer.MAX_VALUE);
    }

    private Predicate<String> createSuccessPredicate() {
        return value -> value.equals(FINAL_RESULT);
    }

    private Predicate<Throwable> createErrorPredicate() {
        return error -> error.equals(EXCEPTION_CAUSING_RETRY);
    }

    private SupplierWithException<String, Throwable> createDirectEventOperation(Object... events) {
        final BlockingQueue<Object> queue = new ArrayBlockingQueue<>(events.length);
        queue.addAll(Arrays.asList(events));
        return createOperation(queue);
    }

    private static ScheduledExecutor createExecutor() {
        return new ScheduledExecutorServiceAdapter(SCHEDULED_EXECUTOR_EXTENSION.getExecutor());
    }

    private SupplierWithException<String, Throwable> createOperation(
            BlockingQueue<Object> eventQueue) {
        return () -> {
            callCount.incrementAndGet();

            final Object nextEvent = eventQueue.take();
            if (nextEvent instanceof String) {
                return (String) nextEvent;
            } else if (nextEvent instanceof Throwable) {
                throw (Throwable) nextEvent;
            }

            throw new AssertionError("Invalid event type: " + nextEvent.getClass().getSimpleName());
        };
    }

    /* ********************************************
     * retryOnSuccess
     */

    @Test
    void testRetryOnSuccessWithImmediateSuccessWithoutRetry() {
        final CompletableFuture<String> resultFuture =
                FutureUtils.retryOnSuccess(
                        createDirectEventOperation(FINAL_RESULT),
                        retriesForever(),
                        createSuccessPredicate(),
                        createExecutor());

        assertThatFuture(resultFuture).eventuallySucceeds().isEqualTo(FINAL_RESULT);
        assertThat(callCount.get()).isOne();
    }

    @Test
    void testRetryOnSuccessWithErrorWithoutRetry() {
        final CompletableFuture<String> resultFuture =
                FutureUtils.retryOnSuccess(
                        createDirectEventOperation(FINAL_EXCEPTION),
                        retriesForever(),
                        createSuccessPredicate(),
                        createExecutor());

        assertThatFuture(resultFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .withCause(FINAL_EXCEPTION);
        assertThat(callCount.get()).isOne();
    }

    @Test
    void testRetryOnSuccessWithRetry() {
        final CompletableFuture<String> resultFuture =
                FutureUtils.retryOnSuccess(
                        createDirectEventOperation(INTERMEDIATE_RESULT_CAUSING_RETRY, FINAL_RESULT),
                        retriesForever(),
                        createSuccessPredicate(),
                        createExecutor());

        assertThatFuture(resultFuture).eventuallySucceeds().isEqualTo(FINAL_RESULT);
        assertThat(callCount.get()).isEqualTo(2);
    }

    @Test
    void testRetryOnSuccessWithAbortedRetry() {
        final int numberOfRetries = 2;
        final Object[] events = new Object[numberOfRetries + 1];
        Arrays.fill(events, INTERMEDIATE_RESULT_CAUSING_RETRY);
        final CompletableFuture<String> resultFuture =
                FutureUtils.retryOnSuccess(
                        createDirectEventOperation(events),
                        retries(numberOfRetries),
                        createSuccessPredicate(),
                        createExecutor());

        assertThatFuture(resultFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(FutureUtils.RetryException.class);
        assertThat(callCount.get()).isEqualTo(numberOfRetries + 1);
    }

    /* ********************************************
     * retryOnError
     */

    @Test
    void testRetryOnErrorWithImmediateSuccessWithoutRetry() {
        final CompletableFuture<String> resultFuture =
                FutureUtils.retryOnError(
                        createDirectEventOperation(FINAL_RESULT),
                        retriesForever(),
                        createErrorPredicate(),
                        createExecutor());

        assertThatFuture(resultFuture).eventuallySucceeds().isEqualTo(FINAL_RESULT);
        assertThat(callCount.get()).isOne();
    }

    @Test
    void testRetryOnErrorWithErrorWithoutRetry() {
        final CompletableFuture<String> resultFuture =
                FutureUtils.retryOnError(
                        createDirectEventOperation(FINAL_EXCEPTION),
                        retriesForever(),
                        createErrorPredicate(),
                        createExecutor());

        assertThatFuture(resultFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(FINAL_EXCEPTION.getClass());
        assertThat(callCount.get()).isOne();
    }

    @Test
    void testRetryOnErrorWithRetry() {
        final CompletableFuture<String> resultFuture =
                FutureUtils.retryOnError(
                        createDirectEventOperation(EXCEPTION_CAUSING_RETRY, FINAL_RESULT),
                        retriesForever(),
                        createErrorPredicate(),
                        createExecutor());

        assertThatFuture(resultFuture).eventuallySucceeds().isEqualTo(FINAL_RESULT);
        assertThat(callCount.get()).isEqualTo(2);
    }

    @Test
    void testRetryOnErrorWithAbortedRetry() {
        final CompletableFuture<String> resultFuture =
                FutureUtils.retryOnError(
                        createDirectEventOperation(
                                EXCEPTION_CAUSING_RETRY, EXCEPTION_CAUSING_RETRY, FINAL_RESULT),
                        retries(1),
                        createErrorPredicate(),
                        createExecutor());

        assertThatFuture(resultFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(FutureUtils.RetryException.class);
        assertThat(callCount.get()).isEqualTo(2);
    }

    /* ********************************************
     * FutureUtils#retry
     *
     * The following tests should cover the error handling. This should reduce the amount
     * of tests for public scheduling/retry methods that are based on FutureUtils#retry.
     */
    @Test
    void testRetryPositiveSuccess() {
        final int expectedValue = 1337;
        final Predicate<Integer> successPredicate = Predicates.alwaysTrue();
        new RetryContext() {
            {
                runTest(
                        () -> expectedValue,
                        successPredicate,
                        Predicates.alwaysTrue(),
                        () -> {
                            assertThat(positiveSuccessConsumer.get()).isEqualTo(expectedValue);
                            assertThat(negativeSuccessConsumer.get()).isNull();
                            assertThat(positiveErrorConsumer.get()).isNull();
                            assertThat(negativeErrorConsumer.get()).isNull();
                        });
            }
        };
    }

    @Test
    void testRetryNegativeSuccess() {
        final int expectedValue = 1337;
        final Predicate<Integer> successPredicate = Predicates.alwaysFalse();
        new RetryContext() {
            {
                runTest(
                        () -> expectedValue,
                        successPredicate,
                        Predicates.alwaysTrue(),
                        () -> {
                            assertThat(positiveSuccessConsumer.get()).isNull();
                            assertThat(negativeSuccessConsumer.get()).isEqualTo(expectedValue);
                            assertThat(positiveErrorConsumer.get()).isNull();
                            assertThat(negativeErrorConsumer.get()).isNull();
                        });
            }
        };
    }

    @Test
    void testRetryPositiveError() {
        final RuntimeException expectedError = new RuntimeException("Expected error");
        final Predicate<Throwable> errorPredicate = Predicates.alwaysTrue();
        new RetryContext() {
            {
                runTest(
                        () -> {
                            throw expectedError;
                        },
                        Predicates.alwaysTrue(),
                        errorPredicate,
                        () -> {
                            assertThat(positiveSuccessConsumer.get()).isNull();
                            assertThat(negativeSuccessConsumer.get()).isNull();
                            assertThat(positiveErrorConsumer.get()).isEqualTo(expectedError);
                            assertThat(negativeErrorConsumer.get()).isNull();
                        });
            }
        };
    }

    @Test
    void testRetryNegativeError() {
        final RuntimeException expectedError = new RuntimeException("Expected error");
        final Predicate<Throwable> errorPredicate = Predicates.alwaysFalse();
        new RetryContext() {
            {
                runTest(
                        () -> {
                            throw expectedError;
                        },
                        Predicates.alwaysTrue(),
                        errorPredicate,
                        () -> {
                            assertThat(positiveSuccessConsumer.get()).isNull();
                            assertThat(negativeSuccessConsumer.get()).isNull();
                            assertThat(positiveErrorConsumer.get()).isNull();
                            assertThat(negativeErrorConsumer.get()).isEqualTo(expectedError);
                        });
            }
        };
    }

    @Test
    void testRetryWithCancellationException() {
        new RetryContext() {
            {
                runTest(
                        () -> {
                            throw new CancellationException("Expected cancellation");
                        },
                        Predicates.alwaysTrue(),
                        Predicates.alwaysTrue(),
                        () -> {
                            assertThat(positiveSuccessConsumer.get()).isNull();
                            assertThat(negativeSuccessConsumer.get()).isNull();
                            assertThat(positiveErrorConsumer.get()).isNull();
                            assertThat(negativeErrorConsumer.get()).isNull();

                            assertThat(resultFuture)
                                    .failsWithin(Duration.ZERO)
                                    .withThrowableOfType(ExecutionException.class)
                                    .withCauseInstanceOf(FutureUtils.RetryException.class);
                        });
            }
        };
    }

    @Test
    void testCompleteInOperationCall() {
        final int randomFinalValue = 1337;
        testCompletionInOperationCall(ft -> ft.complete(randomFinalValue));
    }

    @Test
    void testCompleteExceptionallyInOperationCall() {
        testCompletionInOperationCall(
                ft ->
                        ft.completeExceptionally(
                                new RuntimeException(
                                        "Completing the resultFuture exceptionally will also cancel the retry.")));
    }

    @Test
    void testCancelInOperationCall() {
        testCompletionInOperationCall(ft -> ft.cancel(false));
    }

    private void testCompletionInOperationCall(
            Consumer<CompletableFuture<Integer>> completeFuture) {
        final OneShotLatch inOperationLatch = new OneShotLatch();
        final ExecutorService executorService =
                java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            new RetryContext() {
                {
                    runTest(
                            () -> {
                                inOperationLatch.awaitQuietly();
                                return 1337;
                            },
                            Predicates.alwaysTrue(),
                            Predicates.alwaysTrue(),
                            () -> {
                                resultFuture.cancel(false);
                                inOperationLatch.trigger();

                                assertThat(positiveSuccessConsumer.get()).isNull();
                                assertThat(negativeSuccessConsumer.get()).isNull();
                                assertThat(positiveErrorConsumer.get()).isNull();
                                assertThat(negativeErrorConsumer.get()).isNull();
                            },
                            executorService);
                }
            };
        } finally {
            executorService.shutdown();
        }
    }

    @Test
    void testCompleteAfterOperationCallAndBeforeIntermediateResultHandling() {
        final int randomFinalValue = 1337;
        testCompletionAfterOperationCallAndBeforeIntermediateResultHandling(
                ft -> ft.complete(randomFinalValue));
    }

    @Test
    void testCompleteExceptionallyAfterOperationCallAndBeforeIntermediateResultHandling() {
        testCompletionAfterOperationCallAndBeforeIntermediateResultHandling(
                ft ->
                        ft.completeExceptionally(
                                new RuntimeException(
                                        "Completing the resultFuture exceptionally will also cancel the retry.")));
    }

    @Test
    void testCancelAfterOperationCallAndBeforeIntermediateResultHandling() {
        testCompletionAfterOperationCallAndBeforeIntermediateResultHandling(ft -> ft.cancel(false));
    }

    private void testCompletionAfterOperationCallAndBeforeIntermediateResultHandling(
            Consumer<CompletableFuture<Integer>> completeFuture) {
        final ManuallyTriggeredScheduledExecutor executor =
                new ManuallyTriggeredScheduledExecutor();
        new RetryContext() {
            {
                runTest(
                        () -> 1337,
                        Predicates.alwaysTrue(),
                        Predicates.alwaysTrue(),
                        () -> {
                            // trigger operation
                            executor.trigger();

                            completeFuture.accept(resultFuture);
                            executor.trigger();

                            // cancelling the retry before handling the intermediate result won't
                            // make trigger intermediate result handling
                            assertThat(positiveSuccessConsumer.get()).isNull();
                            assertThat(negativeSuccessConsumer.get()).isNull();
                            assertThat(positiveErrorConsumer.get()).isNull();
                            assertThat(negativeErrorConsumer.get()).isNull();
                        },
                        executor);
            }
        };
    }

    @Test
    void testCompleteBeforeOperationCall() {
        final int randomFinalValue = 1337;
        testCompletionBeforeOperationCall(ft -> ft.complete(randomFinalValue));
    }

    @Test
    void testCompleteExceptionallyBeforeOperationCall() {
        testCompletionBeforeOperationCall(
                ft ->
                        ft.completeExceptionally(
                                new RuntimeException(
                                        "Completing the resultFuture exceptionally will also cancel the retry.")));
    }

    @Test
    void testCancelBeforeOperationCall() {
        testCompletionBeforeOperationCall(ft -> ft.cancel(false));
    }

    private void testCompletionBeforeOperationCall(
            Consumer<CompletableFuture<Integer>> completeFuture) {
        final AtomicInteger callCounter = new AtomicInteger();
        new RetryContext() {
            {
                // complete the result future before triggering the retry
                completeFuture.accept(resultFuture);

                runTest(
                        callCounter::incrementAndGet,
                        Predicates.alwaysTrue(),
                        Predicates.alwaysTrue(),
                        () -> assertThat(callCounter.get()).isZero());
            }
        };
    }

    @Test
    void testCompleteAfterRetryIsScheduled() {
        final int randomFinalValue = 1337;
        testCompletionAfterRetryIsScheduled(ft -> ft.complete(randomFinalValue));
    }

    @Test
    void testCompleteExceptionallyAfterRetryIsScheduled() {
        testCompletionAfterRetryIsScheduled(
                ft ->
                        ft.completeExceptionally(
                                new RuntimeException(
                                        "Completing the resultFuture exceptionally will also cancel the retry.")));
    }

    @Test
    void testCancelAfterRetryIsScheduled() {
        testCompletionAfterRetryIsScheduled(ft -> ft.cancel(false));
    }

    void testCompletionAfterRetryIsScheduled(Consumer<CompletableFuture<Integer>> completeFuture) {
        final AtomicInteger callCounter = new AtomicInteger();
        final SupplierWithException<Integer, Throwable> operation = callCounter::incrementAndGet;
        final Executor executor = Executors.directExecutor();
        new RetryContext() {
            {
                runTest(
                        operation,
                        Predicates.alwaysTrue(),
                        value -> {
                            // complete while handling the intermediate result
                            completeFuture.accept(resultFuture);

                            FutureUtils.retry(
                                    resultFuture,
                                    operation,
                                    Predicates.alwaysTrue(),
                                    positiveSuccessConsumer::set,
                                    negativeSuccessConsumer::set,
                                    Predicates.alwaysTrue(),
                                    positiveErrorConsumer::set,
                                    negativeErrorConsumer::set,
                                    executor);
                        },
                        Predicates.alwaysTrue(),
                        () ->
                                assertThat(callCounter.get())
                                        .as("The operation should only be triggered once.")
                                        .isOne(),
                        executor);
            }
        };
    }

    private static class RetryContext {
        final CompletableFuture<Integer> resultFuture = new CompletableFuture<>();
        final AtomicReference<Integer> positiveSuccessConsumer = new AtomicReference<>();
        final AtomicReference<Integer> negativeSuccessConsumer = new AtomicReference<>();
        final AtomicReference<Throwable> positiveErrorConsumer = new AtomicReference<>();
        final AtomicReference<Throwable> negativeErrorConsumer = new AtomicReference<>();

        void runTest(
                SupplierWithException<Integer, Throwable> operation,
                Predicate<Integer> successPredicate,
                Predicate<Throwable> errorPredicate,
                Runnable assertion) {
            runTest(
                    operation,
                    successPredicate,
                    errorPredicate,
                    assertion,
                    Executors.directExecutor());
        }

        void runTest(
                SupplierWithException<Integer, Throwable> operation,
                Predicate<Integer> successPredicate,
                Predicate<Throwable> errorPredicate,
                Runnable assertion,
                Executor executor) {
            runTest(
                    operation,
                    successPredicate,
                    positiveSuccessConsumer::set,
                    errorPredicate,
                    assertion,
                    executor);
        }

        void runTest(
                SupplierWithException<Integer, Throwable> operation,
                Predicate<Integer> successPredicate,
                Consumer<Integer> customPositiveSuccessConsumer,
                Predicate<Throwable> errorPredicate,
                Runnable assertion,
                Executor executor) {
            FutureUtils.retry(
                    resultFuture,
                    operation,
                    successPredicate,
                    customPositiveSuccessConsumer,
                    negativeSuccessConsumer::set,
                    errorPredicate,
                    positiveErrorConsumer::set,
                    negativeErrorConsumer::set,
                    executor);
            assertion.run();
        }
    }
}
