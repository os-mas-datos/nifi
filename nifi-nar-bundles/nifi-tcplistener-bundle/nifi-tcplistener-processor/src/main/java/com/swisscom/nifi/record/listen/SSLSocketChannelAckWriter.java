package com.swisscom.nifi.record.listen;

import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannel;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SSLSocketChannelAckWriter implements SocketChannelAckWriter {
    private SSLSocketChannel socketChannel;
    public SSLSocketChannelAckWriter(SSLSocketChannel sslSocketChannel) {
        this.socketChannel = sslSocketChannel;
    }

    @Override
    public synchronized int writeAck(ByteBuffer answer) throws IOException {
        int len = answer.array().length;
        this.socketChannel.write(answer.array());
        return len;
    }

    @Override
    public boolean isClosed() {
        return false;
    }
}
