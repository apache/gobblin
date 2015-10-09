package gobblin.tunnel;

/**
 * Represents the different states a given handler can be in.
 */
 enum HandlerState {
    ACCEPTING,
    CONNECTING,
    READING,
    WRITING
}
