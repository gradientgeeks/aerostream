package org.gradientgeeks.aerostream.common;

/**
 * Thrown when broker rejects produce due to out-of-order sequence number (status code 45).
 */
public class OutOfOrderException extends AeroException {

    public OutOfOrderException(String message) {
        super(message);
    }

    public OutOfOrderException(String message, Throwable cause) {
        super(message, cause);
    }
}
