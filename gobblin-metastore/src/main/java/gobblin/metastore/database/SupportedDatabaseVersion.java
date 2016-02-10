package gobblin.metastore.database;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The supported database version of the class
 */
@Documented
@Retention(value= RetentionPolicy.RUNTIME) @Target(value= ElementType.TYPE)
public @interface SupportedDatabaseVersion {
    /**
     * Whether this version if the default.
     */
    public boolean isDefault();

    /**
     * The version supported by the class
     */
    public String version();
}