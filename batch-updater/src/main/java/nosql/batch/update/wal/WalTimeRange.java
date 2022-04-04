package nosql.batch.update.wal;

import java.util.Objects;

public final class WalTimeRange {
    public final long fromTimestamp;
    public final long toTimestamp;

    public WalTimeRange(long fromTimestamp, long toTimestamp) {
        this.fromTimestamp = fromTimestamp;
        this.toTimestamp = toTimestamp;
    }

    public long getFromTimestamp() {
        return fromTimestamp;
    }

    public long getToTimestamp() {
        return toTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WalTimeRange that = (WalTimeRange) o;
        return fromTimestamp == that.fromTimestamp && toTimestamp == that.toTimestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fromTimestamp, toTimestamp);
    }
}
