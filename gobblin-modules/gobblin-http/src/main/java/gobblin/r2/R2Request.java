package gobblin.r2;

import java.nio.charset.Charset;

import com.linkedin.data.ByteString;
import com.linkedin.r2.message.rest.RestRequest;

import gobblin.async.AsyncRequest;


public class R2Request<D> extends AsyncRequest<D, RestRequest> {
  @Override
  public String toString() {
    RestRequest request = getRawRequest();
    StringBuilder outBuffer = new StringBuilder();
    String endl = "\n";
    outBuffer.append("R2Request Info").append(endl);
    outBuffer.append("type: RestRequest").append(endl);
    outBuffer.append("uri: ").append(request.getURI().toString()).append(endl);
    outBuffer.append("headers: ");
    request.getHeaders().forEach((k, v) ->
        outBuffer.append("[").append(k).append(":").append(v).append("] ")
    );
    outBuffer.append(endl);

    ByteString entity = request.getEntity();
    if (entity != null) {
      outBuffer.append("body: ").append(entity.asString(Charset.defaultCharset())).append(endl);
    }
    return outBuffer.toString();
  }
}
