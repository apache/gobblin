package com.trivago.de.kubernetes.javarunner.dummy;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;

/**
 * Use with -Djava.system.class.loader=...TrvClassLoader
 */
@Deprecated
public final class TrvClassLoader extends URLClassLoader {

    static {
        registerAsParallelCapable();
    }

    public TrvClassLoader(final URL[] urls,
                          final ClassLoader parent) {
        super(urls, parent);
    }

    public TrvClassLoader(final ClassLoader parent) {
        this(new URL[0], parent);
    }

    void add(final URL url) {
        addURL(url);
    }


    /**
     * Required for java agents when this classloader is used as the system classloader.
     */
    @SuppressWarnings("unused")
    private void appendToClassPathForInstrumentation(final String jarFile) throws IOException {
        add(Paths.get(jarFile).toRealPath().toUri().toURL());
    }


    public static TrvClassLoader findAncestor(ClassLoader cl) {
        do {
            if (cl instanceof TrvClassLoader)
                return (TrvClassLoader) cl;
            cl = cl.getParent();
        } while (cl != null);

        return null;
    }


    public static TrvClassLoader getTrvClassLoader() throws IllegalStateException {
        final ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();

        if (systemClassLoader instanceof TrvClassLoader)
            return (TrvClassLoader) systemClassLoader;

        throw new IllegalStateException("system classloader is not an instance of " + TrvClassLoader.class.getName()
                + ", it is instead: " + systemClassLoader.getClass().getName());
    }

}
