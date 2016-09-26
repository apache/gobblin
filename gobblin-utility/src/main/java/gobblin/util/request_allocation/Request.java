package gobblin.util.request_allocation;

public interface Request<T extends Request> {
  Requestor<T> getRequestor();
}
