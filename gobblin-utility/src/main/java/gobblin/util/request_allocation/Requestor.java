package gobblin.util.request_allocation;

import java.io.IOException;
import java.util.Iterator;


/**
 * A wrapper around a {@link Iterator} of {@link Request}s used for request allocation problem. See {@link RequestAllocator}.
 */
public interface Requestor<T extends Request> {
  Iterator<T> getRequests() throws IOException;
}
