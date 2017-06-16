package gobblin.http;

import java.io.IOException;
import java.util.Arrays;

import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.util.EntityUtils;

import gobblin.async.AsyncRequest;


public class ApacheHttpRequest<D> extends AsyncRequest<D, HttpUriRequest> {
  @Override
  public String toString() {
    HttpUriRequest request = getRawRequest();
    StringBuilder outBuffer = new StringBuilder();
    String endl = "\n";
    outBuffer.append("ApacheHttpRequest Info").append(endl);
    outBuffer.append("type: HttpUriRequest").append(endl);
    outBuffer.append("uri: ").append(request.getURI().toString()).append(endl);
    outBuffer.append("headers: ");
    Arrays.stream(request.getAllHeaders()).forEach(header ->
        outBuffer.append("[").append(header.getName()).append(":").append(header.getValue()).append("] ")
    );
    outBuffer.append(endl);

    if (request instanceof HttpEntityEnclosingRequest) {
      try {
        String body = EntityUtils.toString(((HttpEntityEnclosingRequest) request).getEntity());
        outBuffer.append("body: ").append(body).append(endl);
      } catch (IOException e) {
        outBuffer.append("body: ").append(e.getMessage()).append(endl);
      }
    }
    return outBuffer.toString();
  }
}
