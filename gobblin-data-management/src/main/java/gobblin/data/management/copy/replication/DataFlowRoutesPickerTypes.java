package gobblin.data.management.copy.replication;

/**
 * An enumeration of possible DataFlowRoutesPicker types 
 * 
 * @author mitu
 *
 */
public enum DataFlowRoutesPickerTypes {

  BY_SOURCE_CLUSTER("by_Source_Cluster");

  private final String name;

  DataFlowRoutesPickerTypes(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return this.name;
  }

  /**
   * Get a {@link DataFlowRoutesPickerTypes} for the given name.
   *
   * @param name the given name
   * @return a {@link DataFlowRoutesPickerTypes} for the given name
   */
  public static DataFlowRoutesPickerTypes forName(String name) {
    return DataFlowRoutesPickerTypes.valueOf(name.toUpperCase());
  }

}
