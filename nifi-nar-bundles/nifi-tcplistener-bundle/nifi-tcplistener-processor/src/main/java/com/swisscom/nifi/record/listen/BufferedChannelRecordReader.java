package com.swisscom.nifi.record.listen;

public interface BufferedChannelRecordReader extends SocketChannelRecordReader, SocketChannelAckWriter{
    void initNewBatch();

    long getBatchAge();
    long getReceiveAge();
    public void resetReceiveTS();
    public org.apache.commons.io.output.QueueOutputStream receiverOutputStream();
    public boolean isIdle();
    public void requestClose();
}
