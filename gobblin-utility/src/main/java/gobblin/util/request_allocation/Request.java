package gobblin.util.request_allocation;

/**
 * Represents an expensive request in the request allocation problem. See {@link RequestAllocator}.
 */
public interface Request<T extends Request> {
  Requestor<T> getRequestor();
}
