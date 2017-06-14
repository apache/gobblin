package gobblin.http;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ApacheHttpResponseStatus extends ResponseStatus {
  private int statusCode;
  private byte[] content = null;
  private String contentType = null;
  public ApacheHttpResponseStatus(StatusType type) {
    super(type);
  }

}
