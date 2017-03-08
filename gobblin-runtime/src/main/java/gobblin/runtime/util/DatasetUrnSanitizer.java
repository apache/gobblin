package gobblin.runtime.util;

import com.google.common.base.CharMatcher;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DatasetUrnSanitizer {
  public static Optional<String> sanitize(final String datasetUrn) {
    if (Strings.isNullOrEmpty(datasetUrn)) {
      return Optional.absent();
    }
    return Optional.of(CharMatcher.is(':').replaceFrom(datasetUrn, '.'));
  }
}
