package org.gradientgeeks.aerostream.common;

/**
 * Thrown when network transport or socket connectivity fails.
 */
public class ConnectionException extends AeroException {

    public ConnectionException(String message) {
        super(message);
    }

    public ConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
