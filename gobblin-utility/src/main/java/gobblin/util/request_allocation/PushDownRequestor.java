package gobblin.util.request_allocation;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;


public interface PushDownRequestor<T extends Request> extends Requestor<T> {
  Iterator<T> getRequests(Comparator<T> prioritizer) throws IOException;
}
