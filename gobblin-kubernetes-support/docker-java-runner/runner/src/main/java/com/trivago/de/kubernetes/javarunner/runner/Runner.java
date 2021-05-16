package com.trivago.de.kubernetes.javarunner.runner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public final class Runner {

    public static void main(final String... args) throws Throwable {
        if (args.length == 0)
            throw new IllegalArgumentException("expecting a class name, got none");

        final String klass = args[0];

        if (Runner.class.getName().equals(klass))
            throw new RuntimeException("can not start self");

        final String[] appArgs;
        if (args.length == 1) {
            appArgs = new String[0];
        }
        else {
            appArgs = new String[args.length - 1];
            System.arraycopy(args, 1, appArgs, 0, appArgs.length);
        }

        final Class<?> k = Class.forName(klass);
        final Method method = k.getMethod("main", String[].class);
        method.setAccessible(true);

        try {
            method.invoke(null, (Object) appArgs);
        }
        catch (final InvocationTargetException e) {
            if (e.getCause() != null)
                throw e.getCause();
            else
                throw e;
        }
    }

}
