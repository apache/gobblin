package gobblin.config.store.api;

/**
 * ConfigStoreWithStableVersion is used to indicate that the configuration store support stable version. 
 * 
 * If the {@ConfigClient} is constructed with policy {@ConfigVersion.VersionStabilityPolicy.RAISE_ERROR},
 * then that {@ConfigClient} can only connected to the {@ConfigStoreWithStableVersion} configuration store.
 * 
 * 
 * Once the version been published to the configuration store, the content will not be changed
 * @author mitu
 *
 */
public interface ConfigStoreWithStableVersion {

}
