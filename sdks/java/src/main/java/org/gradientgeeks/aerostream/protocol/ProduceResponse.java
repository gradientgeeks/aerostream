package org.gradientgeeks.aerostream.protocol;

/**
 * Command 1 (PRODUCE) response data structure.
 */
public final class ProduceResponse {

    private final byte status;
    private final long offset;

    public ProduceResponse(byte status, long offset) {
        this.status = status;
        this.offset = offset;
    }

    public byte status() {
        return status;
    }

    public long offset() {
        return offset;
    }

    public boolean isOk() {
        return status == ProtocolConstants.STATUS_OK;
    }

    @Override
    public String toString() {
        return "ProduceResponse{" +
                "status=" + status +
                ", offset=" + offset +
                '}';
    }
}
