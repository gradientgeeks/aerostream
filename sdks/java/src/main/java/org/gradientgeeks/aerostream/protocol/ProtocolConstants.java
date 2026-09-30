package org.gradientgeeks.aerostream.protocol;

/**
 * Binary protocol constants for AeroStream native framing (0xAE 0x01).
 */
public final class ProtocolConstants {

    public static final byte MAGIC_0 = (byte) 0xAE;
    public static final byte MAGIC_1 = (byte) 0x01;
    public static final byte[] MAGIC = new byte[] {MAGIC_0, MAGIC_1};

    public static final int HEADER_LEN = 7;

    public static final byte CMD_AUTH = 0;
    public static final byte CMD_PRODUCE = 1;
    public static final byte CMD_FETCH = 2;
    public static final byte CMD_REPLICA_FETCH = 3;
    public static final byte CMD_FETCH_MULTI = 4;

    public static final byte STATUS_OK = 0;
    public static final byte STATUS_EMPTY = 1;
    public static final byte STATUS_DATA = 2;
    public static final byte STATUS_AUTH_FAILED = 3;
    public static final byte STATUS_OUT_OF_ORDER = 45;

    public static final int MAX_TOPIC_LENGTH = 65535;

    private ProtocolConstants() {
    }

    /**
     * Checks if the given two bytes match the AeroStream protocol magic bytes.
     */
    public static boolean isValidMagic(byte b0, byte b1) {
        return b0 == MAGIC_0 && b1 == MAGIC_1;
    }
}
