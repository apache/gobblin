package gobblin.metastore.util;

import org.apache.commons.io.FilenameUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import lombok.AllArgsConstructor;
import lombok.Getter;


@AllArgsConstructor
public class StateStoreTableInfo {
  public static final String CURRENT_NAME = "current";
  public static final String TABLE_PREFIX_SEPARATOR = "-";

  @Getter
  private String prefix;

  @Getter
  private boolean isCurrent;

  public static StateStoreTableInfo get(String tableName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "Table name is null or empty.");
    String name = FilenameUtils.getBaseName(tableName);
    if (CURRENT_NAME.equalsIgnoreCase(name)) {
      return new StateStoreTableInfo("", true);
    }
    int suffixIndex = name.lastIndexOf(TABLE_PREFIX_SEPARATOR);
    if (suffixIndex >= 0) {
      String prefix = suffixIndex > 0 ? name.substring(0, suffixIndex) : "";
      String suffix = suffixIndex < name.length() - 1 ? name.substring(suffixIndex + 1) : "";
      if (CURRENT_NAME.equalsIgnoreCase(suffix)) {
        return new StateStoreTableInfo(prefix, true);
      } else {
        return new StateStoreTableInfo(prefix, false);
      }
    }
    return new StateStoreTableInfo("", false);
  }
}
