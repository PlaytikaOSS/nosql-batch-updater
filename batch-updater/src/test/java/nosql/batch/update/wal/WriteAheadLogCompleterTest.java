package nosql.batch.update.wal;

import nosql.batch.update.BatchOperations;
import nosql.batch.update.BatchUpdate;
import nosql.batch.update.lock.Lock;
import nosql.batch.update.lock.PermanentLockingException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class WriteAheadLogCompleterTest {

    private static final long WAIT_TIMEOUT_IN_NANOS = Duration.ofSeconds(10).toNanos();
    private static final Duration STALE_BATCHES_THRESHOLD = Duration.ofSeconds(3600);
    private static final int BATCH_SIZE = 2;

    @Mock
    private BatchOperations<Integer, Integer, Lock, Integer> batchOperations;
    @Mock
    private WriteAheadLogManager<Integer, Integer, Integer> writeAheadLogManager;
    @Mock
    private ExclusiveLocker exclusiveLocker;
    @Mock
    private ScheduledExecutorService scheduledExecutorService;

    private WriteAheadLogCompleter<Integer, Integer, Lock, Integer> writeAheadLogCompleter;


    @Before
    public void setUp() {
        when(batchOperations.getWriteAheadLogManager()).thenReturn(writeAheadLogManager);

        writeAheadLogCompleter = new WriteAheadLogCompleter<>(batchOperations,
                                                              STALE_BATCHES_THRESHOLD,
                                                              BATCH_SIZE,
                                                              exclusiveLocker,
                                                              scheduledExecutorService);
    }

    @After
    public void tearDown() {
        verify(batchOperations).getWriteAheadLogManager();
        verifyNoMoreInteractions(batchOperations, writeAheadLogManager, exclusiveLocker, scheduledExecutorService);
    }

    @Test
    public void shouldStartAndShutdownWalCompleter() throws Exception {
        // given
        long period = STALE_BATCHES_THRESHOLD.toMillis() + 100;
        long halfTimeoutNanos = WAIT_TIMEOUT_IN_NANOS / 2;
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);

        doReturn(scheduledFuture).when(scheduledExecutorService)
                                 .scheduleAtFixedRate(any(Runnable.class), eq(0L), eq(period), eq(TimeUnit.MILLISECONDS));

        // when
        writeAheadLogCompleter.start();
        writeAheadLogCompleter.shutdown();

        // then
        verify(scheduledExecutorService).scheduleAtFixedRate(any(Runnable.class), eq(0L), eq(period), eq(TimeUnit.MILLISECONDS));
        verify(scheduledFuture).cancel(true);
        verify(exclusiveLocker).release();
        verify(exclusiveLocker).shutdown();
        verify(scheduledExecutorService).shutdown();
        verify(scheduledExecutorService, times(2)).awaitTermination(halfTimeoutNanos, TimeUnit.NANOSECONDS);
        verify(scheduledExecutorService).shutdownNow();
        verify(scheduledExecutorService).isTerminated();

        verifyNoMoreInteractions(scheduledFuture);
    }


    @Test
    public void shouldSuspendAndResumeWalCompleter() {
        // when
        writeAheadLogCompleter.suspend();

        // then
        assertThat(writeAheadLogCompleter.isSuspended()).isTrue();
        verify(exclusiveLocker).release();

        // and then
        writeAheadLogCompleter.resume();

        assertThat(writeAheadLogCompleter.isSuspended()).isFalse();
    }

    @Test
    public void shouldCompleteHangedTransactionsForSuspendedWalCompleter() {
        // given
        CompletionStatistic expected = new CompletionStatistic(0, 0, 0, 0);
        writeAheadLogCompleter.suspend();

        // when
        CompletionStatistic actual = writeAheadLogCompleter.completeHangedTransactions();

        // then
        assertThat(actual).isEqualTo(expected);
        verify(exclusiveLocker).release();
    }

    @Test
    public void shouldCompleteHangedTransactionsWhenCannotAcquireLock() {
        // given
        CompletionStatistic expected = new CompletionStatistic(0, 0, 0, 0);
        when(exclusiveLocker.acquire()).thenReturn(false);

        // when
        CompletionStatistic actual = writeAheadLogCompleter.completeHangedTransactions();

        // then
        assertThat(actual).isEqualTo(expected);
        verify(exclusiveLocker).acquire();
    }

    @Test
    public void shouldCompleteHangedTransactions() {
        // given
        long timeMillis = System.currentTimeMillis();
        long timestamp1 = timeMillis - 2000L;
        long timestamp2 = timeMillis - 1001L;
        long timestamp3 = timeMillis - 1000L;
        WalTimeRange range1 = new WalTimeRange(timestamp1, timestamp2);
        WalTimeRange range2 = new WalTimeRange(timestamp3, timeMillis);
        WalRecord<Integer, Integer, Integer> record1 = new WalRecord<>(1, timestamp1, TestBatchUpdate.of(2, 3));
        WalRecord<Integer, Integer, Integer> record2 = new WalRecord<>(4, timestamp2, TestBatchUpdate.of(5, 6));
        WalRecord<Integer, Integer, Integer> record3 = new WalRecord<>(4, timestamp2, TestBatchUpdate.of(5, 6));
        WalRecord<Integer, Integer, Integer> record4 = new WalRecord<>(10, timeMillis, TestBatchUpdate.of(11, 12));

        List<WalRecord<Integer, Integer, Integer>> staleBatches1 = Arrays.asList(record1, record2);
        List<WalRecord<Integer, Integer, Integer>> staleBatches2 = Arrays.asList(record3, record4);

        CompletionStatistic expected = new CompletionStatistic(4, 2, 1, 1);
        when(exclusiveLocker.acquire()).thenReturn(true);
        when(writeAheadLogManager.getTimeRanges(STALE_BATCHES_THRESHOLD, BATCH_SIZE)).thenReturn(Arrays.asList(range1, range2));
        when(writeAheadLogManager.getStaleBatchesForRange(range1)).thenReturn(staleBatches1);
        when(writeAheadLogManager.getStaleBatchesForRange(range2)).thenReturn(staleBatches2);
        doNothing().when(batchOperations).processAndDeleteTransaction(record1.batchId, record1.batchUpdate, true);
        doThrow(new PermanentLockingException("lock exception"))
                .when(batchOperations).processAndDeleteTransaction(record2.batchId, record2.batchUpdate, true);
        doThrow(RuntimeException.class).when(batchOperations).processAndDeleteTransaction(record3.batchId, record3.batchUpdate, true);
        doNothing().when(batchOperations).processAndDeleteTransaction(record4.batchId, record4.batchUpdate, true);
        doNothing().when(batchOperations).releaseLocksAndDeleteWalTransactionOnError(record2.batchUpdate.locks(), record2.batchId);

        // when
        CompletionStatistic actual = writeAheadLogCompleter.completeHangedTransactions();

        // then
        assertThat(actual).isEqualTo(expected);
        verify(exclusiveLocker, times(5)).acquire();
        verify(writeAheadLogManager).getTimeRanges(STALE_BATCHES_THRESHOLD, BATCH_SIZE);
        verify(writeAheadLogManager).getStaleBatchesForRange(range1);
        verify(writeAheadLogManager).getStaleBatchesForRange(range2);
        verify(batchOperations).processAndDeleteTransaction(record1.batchId, record1.batchUpdate, true);
        verify(batchOperations).processAndDeleteTransaction(record2.batchId, record2.batchUpdate, true);
        verify(batchOperations).processAndDeleteTransaction(record3.batchId, record3.batchUpdate, true);
        verify(batchOperations).processAndDeleteTransaction(record4.batchId, record4.batchUpdate, true);
        verify(batchOperations).releaseLocksAndDeleteWalTransactionOnError(record2.batchUpdate.locks(), record2.batchId);
    }

    private static class TestBatchUpdate implements BatchUpdate<Integer, Integer> {
        private final int locks;
        private final int updates;

        private TestBatchUpdate(int locks, int updates) {
            this.locks = locks;
            this.updates = updates;
        }

        public static TestBatchUpdate of(int locks, int updates) {
            return new TestBatchUpdate(locks, updates);
        }

        @Override
        public Integer locks() {
            return locks;
        }

        @Override
        public Integer updates() {
            return updates;
        }
    }

}
