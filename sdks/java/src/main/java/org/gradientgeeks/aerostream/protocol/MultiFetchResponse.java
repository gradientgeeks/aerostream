package org.gradientgeeks.aerostream.protocol;

import org.gradientgeeks.aerostream.common.AeroRecord;

import java.util.Collections;
import java.util.List;

/**
 * Response data structure for Command 4 (FETCH_MULTI).
 */
public final class MultiFetchResponse {

    private final byte status;
    private final List<AeroRecord> records;

    public MultiFetchResponse(byte status, List<AeroRecord> records) {
        this.status = status;
        this.records = records != null ? Collections.unmodifiableList(records) : Collections.emptyList();
    }

    public static MultiFetchResponse empty() {
        return new MultiFetchResponse(ProtocolConstants.STATUS_EMPTY, Collections.emptyList());
    }

    public byte status() {
        return status;
    }

    public List<AeroRecord> records() {
        return records;
    }

    public boolean isEmpty() {
        return status == ProtocolConstants.STATUS_EMPTY;
    }

    public boolean isData() {
        return status == ProtocolConstants.STATUS_DATA;
    }

    public boolean isAuthFailed() {
        return status == ProtocolConstants.STATUS_AUTH_FAILED;
    }

    @Override
    public String toString() {
        return "MultiFetchResponse{" +
                "status=" + status +
                ", recordsCount=" + records.size() +
                '}';
    }
}
