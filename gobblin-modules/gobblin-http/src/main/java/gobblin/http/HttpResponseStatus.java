package gobblin.http;
import lombok.Getter;
import lombok.Setter;


@Getter @Setter
public class HttpResponseStatus extends ResponseStatus {
  private byte[] content;
  private int statusCode;
  private String contentType;
  public HttpResponseStatus(StatusType type) {
    super(type);
  }

}
