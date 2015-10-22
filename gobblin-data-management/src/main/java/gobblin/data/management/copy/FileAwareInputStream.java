package gobblin.data.management.copy;

import java.io.InputStream;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class FileAwareInputStream {

  private CopyableFile file;
  private InputStream inputStream;

}
