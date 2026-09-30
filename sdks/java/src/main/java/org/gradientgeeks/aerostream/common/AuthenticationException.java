package org.gradientgeeks.aerostream.common;

/**
 * Thrown when broker authentication fails (status code 3).
 */
public class AuthenticationException extends AeroException {

    public AuthenticationException(String message) {
        super(message);
    }

    public AuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }
}
