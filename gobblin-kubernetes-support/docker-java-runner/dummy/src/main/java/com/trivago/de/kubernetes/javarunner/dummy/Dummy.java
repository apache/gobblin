package com.trivago.de.kubernetes.javarunner.dummy;

public final class Dummy {

    public static void main(final String... args) {
        System.out.println("Dummy is working");

        System.out.println("args:");
        for (final String arg : args)
            System.out.println(arg);
    }

}
