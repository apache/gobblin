package com.linkedin.uif.source.workunit;

/**
 * Used for tables that receive changelog incremental updates. Extracts can be
 * full drops or incremental deltas. Full drops will be directly to the
 * published snapshot directory. Incremental changes will be published to the
 * append only directories. Changes will then be consumed from the append only
 * directories and used to refresh the snapshots located in the published
 * snapshot directory.
 *
 * @author kgoodhop
 *
 */
public class SnapshotAppendTable extends Table {

  public SnapshotAppendTable(String namespace, String table, String extractId) {
    super(namespace, table, extractId);
  }

  @Override
  public boolean validateTableAttributes() throws MissingExtractAttributeException {
    if (super.validateTableAttributes()) {

      // primary keys always required
      if (getPrimaryKeys().length == 0)
        throw new MissingExtractAttributeException(
            "SnapshotAppend tables require a primary key in order to apply the changelog.  "
                + "If this table won't have a change log, consider using a different table type. "
                + "You can set the primary key(s) using one of the setters on " + Table.class.getName());

      // due to legacy requirements, full snapshots don't require delta
      // fields and
      // subsequent incremental extracts will indicate the delta field
      if (isFull()) {
        throw new MissingExtractAttributeException(
            "SnapshotAppend tables require a timestamp for full drops.  In most cases, this is when the full extract was started. "
                + "You can set the timestamp using " + Table.class.getName() + ".setFullTrue(long extractFullRunTime)");

      } else {
        throw new MissingExtractAttributeException(
            "SnapshotAppend tables require at least one delta field in order to apply the changelog.  This could be a timestamp, transactionId, scn, or something equivelent. "
                + "You can set the delta field(s) using one of the setters on " + Table.class.getName());
      }
    }
    return true;
  }

}
