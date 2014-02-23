package com.linkedin.uif.source.workunit;

/**
 * Used for tables that receive no changelog incremental updates. Each extract
 * is expected to be a full drop and will be written directly to the published
 * snapshot directory.
 *
 * @author kgoodhop
 *
 */
public class SnapshotOnlyTable extends Table {

  public SnapshotOnlyTable(String namespace, String table, String extractId) {
    super(namespace, table, extractId);
    setFullTrue();
  }

  @Override
  public void validateTableAttributes() throws MissingExtractAttributeException {
    super.validateTableAttributes();
    if (getFullExtractRunTime() == -1)
      throw new MissingExtractAttributeException(
          "Full snapshot tables require a timestamp for full drops.  In most cases, this is when the full extract was started. "
              + "You can set the timestamp using " + Table.class.getName() + ".setFullTrue(long extractFullRunTime)");
  }

}
