package nosql.batch.update.wal;

import java.util.Objects;

public class CompletionStatistic {

    public final int staleBatchesFound;
    public final int staleBatchesComplete;
    public final int staleBatchesIgnored;
    public final int staleBatchesErrors;

    public CompletionStatistic(int staleBatchesFound, int staleBatchesComplete, int staleBatchesIgnored, int staleBatchesErrors) {
        this.staleBatchesFound = staleBatchesFound;
        this.staleBatchesComplete = staleBatchesComplete;
        this.staleBatchesIgnored = staleBatchesIgnored;
        this.staleBatchesErrors = staleBatchesErrors;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompletionStatistic that = (CompletionStatistic) o;
        return staleBatchesFound == that.staleBatchesFound && staleBatchesComplete == that.staleBatchesComplete && staleBatchesIgnored == that.staleBatchesIgnored && staleBatchesErrors == that.staleBatchesErrors;
    }

    @Override
    public int hashCode() {
        return Objects.hash(staleBatchesFound, staleBatchesComplete, staleBatchesIgnored, staleBatchesErrors);
    }
}
