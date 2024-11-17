package com.swisscom.nifi.record.listen;

public interface BufferedChannelRecordReader extends SocketChannelRecordReader{
    public java.nio.channels.WritableByteChannel receiverChannel();
    public org.apache.commons.io.output.QueueOutputStream receiverOutputStream();
    public boolean isIdle();
}
