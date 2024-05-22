package nosql.batch.update.aerospike.lock;


import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import nosql.batch.update.aerospike.basic.AerospikeBasicExpectedValueOperations;
import nosql.batch.update.aerospike.lock.AerospikeLockOperations.LockResult;
import nosql.batch.update.lock.Lock;
import nosql.batch.update.lock.TemporaryLockingException;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static nosql.batch.update.aerospike.AerospikeTestUtils.AEROSPIKE_PROPERTIES;
import static nosql.batch.update.aerospike.AerospikeTestUtils.getAerospikeClient;
import static nosql.batch.update.aerospike.AerospikeTestUtils.getAerospikeContainer;
import static nosql.batch.update.aerospike.wal.AerospikeWriteAheadLogManager.generateBatchId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AerospikeLockOperationsTest {

    static final GenericContainer aerospike = getAerospikeContainer();

    static final AerospikeClient client = getAerospikeClient(aerospike);

    static final AerospikeLockOperations aerospikeLockOperations = new AerospikeLockOperations(
            client, new AerospikeBasicExpectedValueOperations(client), Executors.newFixedThreadPool(2));

    private static int keyIncremental = 0;

    @Test
    public void shouldLockKey(){
        Key key = new Key(AEROSPIKE_PROPERTIES.getNamespace(), "lock", Integer.toString(keyIncremental++));
        Value batchId = generateBatchId();
        AerospikeLock lock = aerospikeLockOperations.putLock(batchId, key, true);
        assertThat(lock.key).isEqualTo(key);

        Value batchIdOfLock = aerospikeLockOperations.getBatchIdOfLock(key);
        assertThat(batchIdOfLock).isEqualTo(batchId);
        assertThat(batchIdOfLock.toString()).isEqualTo(batchId.toString());
    }

    @Test
    public void shouldFailIfLockTheSameKey(){
        Key key = new Key(AEROSPIKE_PROPERTIES.getNamespace(), "lock", Integer.toString(keyIncremental++));
        Value batchId = generateBatchId();
        AerospikeLock lock = aerospikeLockOperations.putLock(batchId, key, false);
        assertThat(lock.key).isEqualTo(key);

        Value batchId2 = generateBatchId();
        assertThatThrownBy(() -> aerospikeLockOperations.putLock(batchId2, key, false))
                .hasMessageContaining("Locked by concurrent update")
                .hasMessageContaining(batchId.toString())
                .hasMessageContaining(batchId2.toString());

        Value batchIdOfLock = aerospikeLockOperations.getBatchIdOfLock(key);
        assertThat(batchIdOfLock).isEqualTo(batchId);
        assertThat(batchIdOfLock.toString()).isEqualTo(batchId.toString());
    }

    @Test
    public void shouldFailIfLockTheSameKeyInWal(){
        Key key = new Key(AEROSPIKE_PROPERTIES.getNamespace(), "lock", Integer.toString(keyIncremental++));
        Value batchId = generateBatchId();
        AerospikeLock lock = aerospikeLockOperations.putLock(batchId, key, true);
        assertThat(lock.key).isEqualTo(key);

        Value batchId2 = generateBatchId();
        assertThatThrownBy(() -> aerospikeLockOperations.putLock(batchId2, key, true))
                .hasMessageContaining("Locked by other batch update")
                .hasMessageContaining(batchId.toString())
                .hasMessageContaining(batchId2.toString());

        Value batchIdOfLock = aerospikeLockOperations.getBatchIdOfLock(key);
        assertThat(batchIdOfLock).isEqualTo(batchId);
        assertThat(batchIdOfLock.toString()).isEqualTo(batchId.toString());
    }

    @Test
    public void shouldSuccess(){

        Key key1 = new Key("ns", "set", "1");
        Key key2 = new Key("ns", "set", "2");

        List<CompletableFuture<LockResult<AerospikeLock>>> lockResults = Arrays.asList(
                completedFuture(new LockResult<>(new AerospikeLock(Lock.LockType.LOCKED, key1))),
                completedFuture(new LockResult<>(new AerospikeLock(Lock.LockType.SAME_BATCH, key2))));

        List<AerospikeLock> locked = AerospikeLockOperations.processResults(lockResults);
        assertThat(locked).containsExactly(
                new AerospikeLock(Lock.LockType.LOCKED, key1),
                new AerospikeLock(Lock.LockType.SAME_BATCH, key2));
    }

    @Test(expected = TemporaryLockingException.class)
    public void shouldFail(){

        Key keyLocked = new Key("ns", "set", "1");

        List<CompletableFuture<LockResult<AerospikeLock>>> lockResults = Arrays.asList(
                completedFuture(new LockResult<>(new AerospikeLock(Lock.LockType.LOCKED, keyLocked))),
                completedFuture(new LockResult<>(new TemporaryLockingException("test"))));

        AerospikeLockOperations.processResults(lockResults);
    }

    @Test(expected = RuntimeException.class)
    public void shouldSelectNonLockingError(){

        Key keyLocked = new Key("ns", "set", "1");

        List<CompletableFuture<LockResult<AerospikeLock>>> lockResults = Arrays.asList(
                completedFuture(new LockResult<>(new AerospikeLock(Lock.LockType.LOCKED, keyLocked))),
                completedFuture(new LockResult<>(new TemporaryLockingException("test"))),
                completedFuture(new LockResult<>(new SocketTimeoutException("test"))));

        AerospikeLockOperations.processResults(lockResults);
    }


}
