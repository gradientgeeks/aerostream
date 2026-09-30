package org.gradientgeeks.aerostream.common;

/**
 * Thrown when an operation exceeds its configured deadline or timeout.
 */
public class TimeoutException extends AeroException {

    public TimeoutException(String message) {
        super(message);
    }

    public TimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
