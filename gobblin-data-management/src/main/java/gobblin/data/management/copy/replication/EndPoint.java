package gobblin.data.management.copy.replication;

import gobblin.source.extractor.Watermark;


/**
 * Used to encapsulate all the information of a replica, including original source, during replication process.
 * 
 * <ul>
 *  <li>Configuration of the data reside on this replica
 *  <li>Whether the replica is original source
 *  <li>The {@link Watermark} of this replica
 * </ul>
 * @author mitu
 *
 */
public interface EndPoint {

  /**
   * @return true iff this represents the original source
   */
  public boolean isSource();

  /**
   * @return the end point name 
   */
  public String getEndPointName();

  /**
   * 
   * @return the {@link Watermark} of the replica
   */
  public Watermark getWatermark();
}
