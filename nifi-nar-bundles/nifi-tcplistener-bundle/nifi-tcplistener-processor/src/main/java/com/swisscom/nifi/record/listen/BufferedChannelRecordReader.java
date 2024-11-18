package com.swisscom.nifi.record.listen;

public interface BufferedChannelRecordReader extends SocketChannelRecordReader, SocketChannelAckWriter{
    void initNewBatch();

    long getBatchAge();

    public org.apache.commons.io.output.QueueOutputStream receiverOutputStream();
    public boolean isIdle();
}
