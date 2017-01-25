package gobblin.gradle;

/**
 * Encapsulates various aspects of a project property that can be used to customize the build through
 * the gradle -P switch.
 */
public class BuildProperty {
    private final String HELP_FORMAT = "\t%-20s - %s. Default: %s";

    public final String name;
    public final Object defaultValue;
    public final String description;

    public BuildProperty(String name, Object defaultValue, String description) {
        this.name = name;
        this.defaultValue = defaultValue;
        this.description = description;
    }

    public String getHelp() {
        return String.format(HELP_FORMAT, this.name, this.description, this.defaultValue)
    }
}