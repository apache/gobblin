package com.linkedin.uif.configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.uif.source.workunit.Extract;
import com.linkedin.uif.source.workunit.Extract.TableType;
import com.linkedin.uif.source.workunit.WorkUnit;

/**
 * <p>
 * Container for all meta data related to a particular source. This includes all properties 
 * defined in .pull property files and all properties stored by tasks of the previous run. 
 * </p>
 * 
 * @author kgoodhop
 *
 */
public class SourceState extends State
{
  private List<WorkUnitState> previousTaskStates = new ArrayList<WorkUnitState>();
  private static Set<Extract> extractSet = Collections.synchronizedSet(new HashSet<Extract>());
  private static DateTimeFormatter DTF = DateTimeFormat.forPattern("yyyyMMddHHmmss").withLocale(Locale.US).withZone(DateTimeZone.UTC);

  /**
   * default constructor
   */
  public SourceState()
  {
  }

  /**
   * 
   * @param properties <p>properties defined in the .pull file</p>
   * @param previousTaskStates <p>properties stored the tasks of the previous run for this source</p>
   */
  public SourceState(State properties, List<WorkUnitState> previousTaskStates)
  {
    addAll(properties);
    this.previousTaskStates.addAll(previousTaskStates);
  }

  /**
   * 
   * @return list of {@link WorkUnitState} from the previous run
   */
  public List<WorkUnitState> getPreviousStates()
  {
    return previousTaskStates;
  }
  
  /**
   * <p>
   * Builder for {@link Extract} that correctly populates the instance
   * The create extract method should always return a unique extract
   * @param type {@link TableType} 
   * @param namespace namespace of the table this extract belongs to
   * @param table name of table this extract belongs to
   * @return
   */
  public synchronized Extract createExtract(TableType type, String namespace, String table)
  {
    Extract extract = new Extract(this, type, namespace, table);
    while (extractSet.contains(extract)) {
        DateTime extractDateTime = DTF.parseDateTime(extract.getExtractId());
        extract.setExtractId(DTF.print(extractDateTime.plusSeconds(1)));
    }
    extractSet.add(extract);
    return extract;
  }

  /**
   * builder for WorkUnit that correctly populates WorkUnit from config if needed
   * @param extract {@link Extract}
   * @return
   */
  public WorkUnit createWorkUnit(Extract extract){
    return new WorkUnit(this, extract);
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeInt(previousTaskStates.size());

    for (WorkUnitState state : previousTaskStates)
    {
      state.write(out);
    }
    super.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException
  {
    int size = in.readInt();

    for (int i = 0; i < size; i++)
    {
      WorkUnitState state = new WorkUnitState();
      state.readFields(in);

      previousTaskStates.add(state);
    }
    super.readFields(in);
  }

}
