package gobblin.writer.http;

import lombok.AllArgsConstructor;
import lombok.Getter;

import gobblin.writer.WriteCallback;


/**
 * This class represents a record in a buffer
 */
@AllArgsConstructor
@Getter
public class BufferedRecord<D> {
  private final D record;
  private final WriteCallback callback;
}
