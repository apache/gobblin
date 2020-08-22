public class CloseBeforeFlushException extends RuntimeException {
  String datasetName;

  public CloseBeforeFlushException(String datasetName) {
    super(String.format("Dataset %s has an attempt to close writer before buffered data to be flushed", datasetName));
  }

  public CloseBeforeFlushException(String datasetName, Throwable cause) {
    super(String.format("Dataset %s has an attempt to close writer before buffered data to be flushed", datasetName),
        cause);
  }
}