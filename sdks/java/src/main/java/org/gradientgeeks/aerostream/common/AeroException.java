package org.gradientgeeks.aerostream.common;

/**
 * Base exception for all AeroStream client and protocol errors.
 */
public class AeroException extends RuntimeException {

    public AeroException(String message) {
        super(message);
    }

    public AeroException(String message, Throwable cause) {
        super(message, cause);
    }
}
