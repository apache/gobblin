package gobblin.util.request_allocation;

import java.io.IOException;
import java.util.Iterator;


public interface Requestor<T extends Request> {
  Iterator<T> getRequests() throws IOException;
}
