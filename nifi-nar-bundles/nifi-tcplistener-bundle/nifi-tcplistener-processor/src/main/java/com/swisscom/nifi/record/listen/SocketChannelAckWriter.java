package com.swisscom.nifi.record.listen;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface SocketChannelAckWriter {
    public int writeAck(ByteBuffer answer) throws IOException;
    public boolean isClosed();
}
