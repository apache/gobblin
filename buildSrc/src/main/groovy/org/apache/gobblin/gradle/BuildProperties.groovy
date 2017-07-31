package org.apache.gobblin.gradle;

import java.util.TreeMap
import org.gradle.api.Project

/**
 * The manages the collection of all known build properties for the project. It is
 * essentially a map from the property name to the BuildProperty object.
 */
public class BuildProperties extends TreeMap<String, BuildProperty> {
    final Project project;

    public BuildProperties(Project project) {
        super();
        this.project = project
    }

    public BuildProperties register(BuildProperty prop) {
        put(prop.name, prop);
        return this;
    }

    public void ensureDefined(String propName) {
        if (! containsKey(propName)) {
            throw new RuntimeException ("Property not defined: " + propName)
        }
        def defaultValue = get(propName).defaultValue

        // Special treatment for Boolean flags -- just specifying the property
        // is treated as setting to true.
        if (null != defaultValue && defaultValue instanceof Boolean &&
                !((Boolean)defaultValue).booleanValue()) {
            this.project.ext.set(propName, this.project.hasProperty(propName))
        }
        else if (! this.project.hasProperty(propName)) {
            this.project.ext.set(propName, defaultValue)
        }

        println String.format("Build property: %s=%s", propName, this.project.ext.get(propName))
    }

    public void printHelp() {
        println "\n\n"
        println "BUILD PROPERTIES"
        println ""
        this.each { propName, propHelp ->
            println propHelp.getHelp()
        }
    }
}