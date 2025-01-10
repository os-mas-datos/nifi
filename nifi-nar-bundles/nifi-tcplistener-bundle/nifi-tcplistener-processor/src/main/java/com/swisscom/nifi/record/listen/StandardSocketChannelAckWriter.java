package com.swisscom.nifi.record.listen;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class StandardSocketChannelAckWriter implements SocketChannelAckWriter {
    private SocketChannel socketChannel;
    public StandardSocketChannelAckWriter(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }
    @Override
    public synchronized int writeAck(ByteBuffer buffer) throws IOException {
        buffer.flip(); // TODO: why ?
            while (buffer.hasRemaining()) {
                return socketChannel.write(buffer);
            }
        return 0;
    }

    @Override
    public boolean isClosed() {
        return false;
    }
}
