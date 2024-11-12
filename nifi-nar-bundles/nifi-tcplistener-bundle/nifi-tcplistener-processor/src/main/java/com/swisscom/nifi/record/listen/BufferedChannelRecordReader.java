package com.swisscom.nifi.record.listen;

public interface BufferedChannelRecordReader extends SocketChannelRecordReader{
    public java.nio.channels.WritableByteChannel receiverChannel();
    public boolean isIdle();
}
