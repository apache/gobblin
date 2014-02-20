package com.linkedin.uif.source.workunit;

/**
 * Used for tables that receives no updates, only inserts. Output data will be
 * date partitioned only.
 *
 * @author kgoodhop
 *
 */
public class AppendOnlyTable extends Table {

  public AppendOnlyTable(String namespace, String table, String extractId) {
    super(namespace, table, extractId);
  }

  @Override
  public boolean validateTableAttributes() throws MissingExtractAttributeException {
    if (super.validateTableAttributes() && getDeltaFields().length == 0) {
      throw new MissingExtractAttributeException(
          "Append tables require at least one delta field in order to partition the data according to data/time.  This should be a timestamp or something equivelent. "
              + "You can set the delta field(s) using one of the setters on " + Table.class.getName());
    }
    return true;
  }

}
