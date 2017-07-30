package gobblin.async;

public interface Callback<T> {

  void onSuccess(T result);

  void onFailure(Throwable throwable);
}
