package com.trivago.de.kubernetes.javarunner.dummy;

public final class DummyFailing {

    public static void main(final String... args) {
        throw new RuntimeException("dummy failure");
    }

}
