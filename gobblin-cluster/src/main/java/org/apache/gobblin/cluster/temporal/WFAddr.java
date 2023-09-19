package org.apache.gobblin.cluster.temporal;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;


/** Hierarchical address for nesting workflows (0-based). */
@NoArgsConstructor // IMPORTANT: for jackson (de)serialization
@RequiredArgsConstructor
public class WFAddr {
    public static final String SEP = ".";

    /** initial, top-level address */
    public static final WFAddr ROOT = new WFAddr(0);

    @Getter
    @NonNull // IMPORTANT: for jackson (de)serialization (which won't permit `final`)
    private List<Integer> segments;

    public WFAddr(final int firstLevelOnly) {
        this(Lists.newArrayList(firstLevelOnly));
    }

    /** @return 0-based depth */
    @JsonIgnore // (because no-arg method resembles 'java bean property')
    public int getDepth() {
        return segments.size() - 1;
    }

    /** Create a child of the current `WFAddr` */
    public WFAddr createChild(int childLevel) {
        final List<Integer> copy = new ArrayList<>(segments);
        copy.add(childLevel);
        return new WFAddr(copy);
    }

    @Override
    public String toString() {
        return Joiner.on(SEP).join(segments);
    }
}
