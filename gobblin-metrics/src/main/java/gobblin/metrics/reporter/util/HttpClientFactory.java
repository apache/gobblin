package gobblin.metrics.reporter.util;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class HttpClientFactory extends BasePooledObjectFactory<HttpClient>{

  @Override
  public HttpClient create() throws Exception {
    return new HttpClient();
  }

  @Override
  public PooledObject<HttpClient> wrap(HttpClient obj) {
    return new DefaultPooledObject<>(obj);
  }

}
