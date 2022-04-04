package nosql.batch.update.aerospike.wal;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import nosql.batch.update.BatchUpdate;
import nosql.batch.update.aerospike.lock.AerospikeBatchLocks;
import nosql.batch.update.wal.WalRecord;
import nosql.batch.update.wal.WalTimeRange;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static com.aerospike.client.ResultCode.RECORD_TOO_BIG;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AerospikeWriteAheadLogManagerUnitTest {
    private static final String UUID_BIN_NAME = "uuid";
    private static final String TIMESTAMP_BIN_NAME = "timestamp";
    private static final String WAL_NAMESPACE = "WAL_NAMESPACE";
    private static final String WAL_SET = "WAL_SET";
    private static final String INDEX_NAME = WAL_SET + "_timestamp";

    @Mock
    private IAerospikeClient client;
    @Mock
    private IndexTask indexTask;
    @Mock
    private AerospikeBatchUpdateSerde<TestAerospikeBatchLocks, Integer, Integer> batchSerializer;
    @Mock
    private Clock clock;

    private AerospikeWriteAheadLogManager<TestAerospikeBatchLocks, Integer, Integer> aerospikeWriteAheadLogManager;

    @Before
    public void setUp() {
        when(client.getWritePolicyDefault()).thenReturn(new WritePolicy());
        when(client.createIndex(null, WAL_NAMESPACE, WAL_SET, INDEX_NAME, TIMESTAMP_BIN_NAME, IndexType.NUMERIC))
                .thenReturn(indexTask);

        aerospikeWriteAheadLogManager = new AerospikeWriteAheadLogManager<>(client, WAL_NAMESPACE, WAL_SET, batchSerializer, clock);
    }

    @After
    public void tearDown() {
        verify(client).getWritePolicyDefault();
        verify(client).createIndex(null, WAL_NAMESPACE, WAL_SET, INDEX_NAME, TIMESTAMP_BIN_NAME, IndexType.NUMERIC);
        verify(indexTask).waitTillComplete(200, 0);

        verifyNoMoreInteractions(client, batchSerializer, clock);
    }

    @Test
    public void shouldReturnWalNamespace() {
        // when
        String actual = aerospikeWriteAheadLogManager.getWalNamespace();

        // then
        assertThat(actual).isEqualTo(WAL_NAMESPACE);
    }

    @Test
    public void shouldReturnWalSet() {
        // when
        String actual = aerospikeWriteAheadLogManager.getWalSetName();

        // then
        assertThat(actual).isEqualTo(WAL_SET);
    }

    @Test
    public void shouldReturnAerospikeClient() {
        // when
        IAerospikeClient actual = aerospikeWriteAheadLogManager.getClient();

        // then
        assertThat(actual).isEqualTo(client);
    }

    @Test
    public void shouldWriteBatch() {
        // given
        long timeMillis = System.currentTimeMillis();
        TestAerospikeBatchLocks locks = TestAerospikeBatchLocks.of(emptyList(), 1);
        BatchUpdate<TestAerospikeBatchLocks, Integer> testBatchUpdate = TestBatchUpdate.of(locks, 2);

        UUID uuid = UUID.randomUUID();
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());

        Value expected = Value.get(bb.array());

        Bin bin1 = new Bin("1", 2);
        Bin bin2 = new Bin("3", 4);
        Bin bin3 = new Bin(UUID_BIN_NAME, expected);
        Bin bin4 = new Bin(TIMESTAMP_BIN_NAME, Value.get(timeMillis));

        when(batchSerializer.write(testBatchUpdate)).thenReturn(Arrays.asList(bin1, bin2));
        when(clock.millis()).thenReturn(timeMillis);

        // when
        try (MockedStatic<UUID> uuidMockedStatic = mockStatic(UUID.class)) {
            uuidMockedStatic.when(UUID::randomUUID).thenReturn(uuid);

            Value actual = aerospikeWriteAheadLogManager.writeBatch(testBatchUpdate);

            assertThat(actual).isEqualTo(expected);
        }

        // then
        verify(batchSerializer).write(testBatchUpdate);
        verify(clock).millis();
        verify(client).put(any(WritePolicy.class), eq(new Key(WAL_NAMESPACE, WAL_SET, expected)), eq(bin1), eq(bin2), eq(bin3), eq(bin4));
    }

    @Test
    public void shouldThrowExceptionDuringWriteBatch() {
        // given
        long timeMillis = System.currentTimeMillis();
        TestAerospikeBatchLocks locks = TestAerospikeBatchLocks.of(emptyList(), 1);
        BatchUpdate<TestAerospikeBatchLocks, Integer> testBatchUpdate = TestBatchUpdate.of(locks, 2);

        when(batchSerializer.write(testBatchUpdate)).thenReturn(emptyList());
        when(clock.millis()).thenReturn(timeMillis);
        doThrow(new AerospikeException(RECORD_TOO_BIG, "Record too big"))
                .when(client).put(any(WritePolicy.class), any(Key.class), any(Bin.class), any(Bin.class));

        // when
        assertThatCode(() -> aerospikeWriteAheadLogManager.writeBatch(testBatchUpdate))
                .isExactlyInstanceOf(AerospikeException.class)
                .hasFieldOrPropertyWithValue("resultCode", RECORD_TOO_BIG)
                .hasMessage("Error 13: Record too big");

        // then
        verify(batchSerializer).write(testBatchUpdate);
        verify(clock).millis();
        verify(client).put(any(WritePolicy.class), any(Key.class), any(Bin.class), any(Bin.class));
    }

    @Test
    public void shouldDeleteBatch() {
        // given
        Value batchId = Value.get("batchId");

        // when
        aerospikeWriteAheadLogManager.deleteBatch(batchId);

        // then
        verify(client).delete(any(WritePolicy.class), eq(new Key(WAL_NAMESPACE, WAL_SET, batchId)));
    }

    @Test
    public void shouldGetTimeRanges() {
        // given
        Duration staleThreshold = Duration.ofSeconds(100);
        int batchSize = 5;

        long timeMillis = System.currentTimeMillis();

        RecordSet recordSet = mock(RecordSet.class);
        List<WalTimeRange> expected = Arrays.asList(new WalTimeRange(1, 5), new WalTimeRange(6, 10));


        Iterator<KeyRecord> iterator = mockTimeKeyRecordsIterator(5, 3, 5, 1, 9, 4, 2, 10, 6, 8, 7);

        when(clock.millis()).thenReturn(timeMillis);
        when(client.query(eq(null), any(Statement.class))).thenReturn(recordSet);
        when(recordSet.iterator()).thenReturn(iterator);

        // when
        List<WalTimeRange> actual = aerospikeWriteAheadLogManager.getTimeRanges(staleThreshold, batchSize);

        // then
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);

        verify(clock).millis();
        verify(client).query(eq(null), any(Statement.class));
    }

    @Test
    public void shouldGetStaleBatchesForRange() {
        // given
        long timeMillis1 = System.currentTimeMillis();
        long timeMillis2 = timeMillis1 + 100;

        RecordSet recordSet = mock(RecordSet.class);
        WalTimeRange range = new WalTimeRange(1, 5);

        Iterator<KeyRecord> iterator = Arrays.asList(mockKeyRecord(1, timeMillis1),
                                                     mockKeyRecord(3, timeMillis2)).iterator();

        TestAerospikeBatchLocks locks1 = TestAerospikeBatchLocks.of(emptyList(), 1);
        TestAerospikeBatchLocks locks2 = TestAerospikeBatchLocks.of(emptyList(), 2);
        TestBatchUpdate batchUpdate1 = TestBatchUpdate.of(locks1, 2);
        TestBatchUpdate batchUpdate2 = TestBatchUpdate.of(locks2, 4);
        List<WalRecord<TestAerospikeBatchLocks, Integer, Value>> expected =
                Arrays.asList(new WalRecord<>(Value.get(1), timeMillis1, batchUpdate1),
                              new WalRecord<>(Value.get(3), timeMillis2, batchUpdate2));

        when(client.query(eq(null), any(Statement.class))).thenReturn(recordSet);
        when(recordSet.iterator()).thenReturn(iterator);
        when(batchSerializer.read(any())).thenReturn(batchUpdate1).thenReturn(batchUpdate2);

        // when
        List<WalRecord<TestAerospikeBatchLocks, Integer, Value>> actual = aerospikeWriteAheadLogManager.getStaleBatchesForRange(range);

        // then
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);

        verify(client).query(eq(null), any(Statement.class));
        verify(batchSerializer, times(2)).read(any());
    }

    private Iterator<KeyRecord> mockTimeKeyRecordsIterator(long... values) {
        List<KeyRecord> list = new ArrayList<>(values.length);

        for (long value : values) {
            Record record = mock(Record.class);
            when(record.getLong(TIMESTAMP_BIN_NAME)).thenReturn(value);
            list.add(new KeyRecord(null, record));
        }

        return list.iterator();
    }

    private KeyRecord mockKeyRecord(int batchId, long timestamp) {
        Record record = mock(Record.class);
        when(record.getValue(UUID_BIN_NAME)).thenReturn(batchId);
        when(record.getLong(TIMESTAMP_BIN_NAME)).thenReturn(timestamp);

        return new KeyRecord(null, record);
    }

    private static class TestAerospikeBatchLocks implements AerospikeBatchLocks<Integer> {

        private final List<Key> keys;
        private final Integer value;

        public TestAerospikeBatchLocks(List<Key> keys, Integer value) {
            this.keys = keys;
            this.value = value;
        }

        public static TestAerospikeBatchLocks of(List<Key> keys, Integer value) {
            return new TestAerospikeBatchLocks(keys, value);
        }

        @Override
        public List<Key> keysToLock() {
            return keys;
        }

        @Override
        public Integer expectedValues() {
            return value;
        }
    }

    private static class TestBatchUpdate implements BatchUpdate<TestAerospikeBatchLocks, Integer> {
        private final TestAerospikeBatchLocks locks;
        private final int updates;

        private TestBatchUpdate(TestAerospikeBatchLocks locks, int updates) {
            this.locks = locks;
            this.updates = updates;
        }

        public static TestBatchUpdate of(TestAerospikeBatchLocks locks, int updates) {
            return new TestBatchUpdate(locks, updates);
        }

        @Override
        public TestAerospikeBatchLocks locks() {
            return locks;
        }

        @Override
        public Integer updates() {
            return updates;
        }
    }

}
