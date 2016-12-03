package gobblin.kafka.serialize;

public class SerializationException extends Exception {
  public SerializationException(String message) {
    super(message);
  }

  public SerializationException(String s, Exception e) {
    super(s, e);
  }

  public SerializationException(Exception e) {
    super(e);
  }
}
