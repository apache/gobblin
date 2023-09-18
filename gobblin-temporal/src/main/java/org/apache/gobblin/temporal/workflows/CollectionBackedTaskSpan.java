package org.apache.gobblin.temporal.workflows;
import java.util.Iterator;
import java.util.List;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;


/** Logical sub-sequence of `Task`s, backed for simplicity's sake by an in-memory collection */
@NoArgsConstructor
@RequiredArgsConstructor
public class CollectionBackedTaskSpan<T> implements Workload.TaskSpan<T> {
    @NonNull
    private List<T> elems;
    // CAUTION: despite the "warning: @NonNull is meaningless on a primitive @lombok.RequiredArgsConstructor"...
    // if removed, no two-arg ctor is generated, so syntax error on `new CollectionBackedTaskSpan(elems, startIndex)`
    @NonNull
    private int startingIndex;
    private transient Iterator<T> statefulDelegatee = null;

    @Override
    public int getNumElems() {
        return elems.size();
    }

    @Override
    public boolean hasNext() {
        if (statefulDelegatee == null) {
            statefulDelegatee = elems.iterator();
        }
        return statefulDelegatee.hasNext();
    }

    @Override
    public T next() {
        if (statefulDelegatee == null) {
            throw new IllegalStateException("first call `hasNext()`!");
        }
        return statefulDelegatee.next();
    }

    @Override
    public String toString() {
        return getClassNickname() + "(" + startingIndex + "... {+" + getNumElems() + "})";
    }

    protected String getClassNickname() {
        // return getClass().getSimpleName();
        return "TaskSpan";
    }
}
