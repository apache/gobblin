package gobblin.util;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import org.apache.commons.lang.StringUtils;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;

public class JvmUtils {
  private static final Joiner JOINER = Joiner.on(" ").skipNulls();

  private static final PortUtils PORT_UTILS = new PortUtils();

  private JvmUtils() {
  }

  /**
   * Gets the input arguments passed to the JVM.
   * @return The input arguments.
   */
  public static String getJvmInputArguments() {
    RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
    List<String> arguments = runtimeMxBean.getInputArguments();
    return String.format("JVM Input Arguments: %s", JOINER.join(arguments));
  }

  /**
   * Formats the specified jvm arguments such that any tokens are replaced with concrete values;
   * @param jvmArguments
   * @return The formatted jvm arguments.
   */
  public static String formatJvmArguments(Optional<String> jvmArguments) {
    if (jvmArguments.isPresent()) {
      return PORT_UTILS.replacePortTokens(jvmArguments.get());
    }
    return StringUtils.EMPTY;
  }
}
