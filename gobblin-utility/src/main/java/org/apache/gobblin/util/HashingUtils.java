package org.apache.gobblin.util;

import com.google.common.hash.Hashing;
import java.net.URI;
import java.nio.charset.StandardCharsets;


public class HashingUtils {
  public static int hashFlowSpec(URI uri, int numSchedulers) {
    return Math.abs(Hashing.sha256().hashString(uri.toString(), StandardCharsets.UTF_8).asInt() % numSchedulers);
  }
}
