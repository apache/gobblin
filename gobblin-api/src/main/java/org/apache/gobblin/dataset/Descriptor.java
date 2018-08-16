package org.apache.gobblin.dataset;

import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import lombok.Getter;
import lombok.RequiredArgsConstructor;


/**
 * A descriptor is a simplified representation of a resource, which could be a dataset, dataset partition, file, etc.
 * It is a digest or abstract, which contains identification properties of the actual resource object, such as ID, name
 * primary keys, version, etc
 *
 * <p>
 *   The class provides {@link #serialize(Descriptor)} and {@link #deserialize(String)} util methods pair to send
 *   a descriptor object over the wire
 * </p>
 *
 * <p>
 *   When the original object has complicated inner structure and there is a requirement to send it over the wire,
 *   it's a time to define a corresponding {@link Descriptor} becomes. In this case, the {@link Descriptor} can just
 *   have minimal information enough to construct the original object on the other side of the wire
 * </p>
 *
 * <p>
 *   When the cost to instantiate the complete original object is high, for example, network calls are required, but
 *   the use cases are limited to the identification properties, define a corresponding {@link Descriptor} becomes
 *   handy
 * </p>
 */
@RequiredArgsConstructor
public class Descriptor {

  /** Use gson for ser/de */
  private static final Gson GSON = new Gson();

  /** Name of the resource */
  @Getter
  private final String name;

  @Override
  public String toString() {
    return GSON.toJson(this);
  }

  public Descriptor copy() {
    return new Descriptor(name);
  }

  /**
   * A helper class for ser/de of a {@link Descriptor}
   */
  @RequiredArgsConstructor
  private static class Wrap {
    /** The actual class name of the {@link Descriptor} */
    private final String clazz;
    /** A json representation of the {@link Descriptor}*/
    private final JsonObject data;
  }

  /**
   * Serialize any {@link Descriptor} object to a string
   */
  public static String serialize(Descriptor descriptor) {
    if (descriptor == null) {
      return GSON.toJson(null);
    }
    JsonObject data = GSON.toJsonTree(descriptor).getAsJsonObject();
    return GSON.toJson(new Wrap(descriptor.getClass().getName(), data));
  }

  /**
   * Deserialize a string, which results from {@link #serialize(Descriptor)}, into the original
   * {@link Descriptor} object
   */
  public static Descriptor deserialize(String serialized) {
    Wrap wrap = GSON.fromJson(serialized, Wrap.class);
    if (wrap == null) {
      return null;
    }

    try {
      return GSON.fromJson(wrap.data, (Type) Class.forName(wrap.clazz));
    } catch (ClassNotFoundException e) {
      return null;
    }
  }
}
