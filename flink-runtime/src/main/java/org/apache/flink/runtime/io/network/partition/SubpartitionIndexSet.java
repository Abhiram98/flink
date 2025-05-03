package org.apache.flink.runtime.io.network.partition;

public class SubpartitionIndexSet {
    private final int start;
    private final int end;

    public SubpartitionIndexSet(int start, int end) {
        this.start = start;
        this.end = end;
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }

    public boolean isEmpty() {
        return start > end;
    }

    public int size() {
        return end - start + 1;
    }
}
