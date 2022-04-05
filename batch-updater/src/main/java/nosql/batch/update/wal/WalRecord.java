package nosql.batch.update.wal;

import nosql.batch.update.BatchUpdate;

import java.util.Objects;

public final class WalRecord<LOCKS, UPDATES, BATCH_ID> implements Comparable<WalRecord<LOCKS, UPDATES, BATCH_ID>> {

    public final BATCH_ID batchId;
    public final long timestamp;
    public final BatchUpdate<LOCKS, UPDATES> batchUpdate;

    public WalRecord(BATCH_ID batchId, long timestamp, BatchUpdate<LOCKS, UPDATES> batchUpdate) {
        this.batchId = batchId;
        this.timestamp = timestamp;
        this.batchUpdate = batchUpdate;
    }

    @Override
    public int compareTo(WalRecord<LOCKS, UPDATES, BATCH_ID> transaction) {
        return Long.compare(timestamp, transaction.timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WalRecord<?, ?, ?> walRecord = (WalRecord<?, ?, ?>) o;
        return timestamp == walRecord.timestamp && Objects.equals(batchId, walRecord.batchId) && Objects.equals(batchUpdate, walRecord.batchUpdate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(batchId, timestamp, batchUpdate);
    }
}
