package com.swisscom.nifi.record.listen;

import org.apache.commons.io.input.QueueInputStream;
import org.apache.commons.io.output.QueueOutputStream;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class StandardBufferChannelRecordReader implements BufferedChannelRecordReader, SocketChannelAckWriter{
    public final BlockingQueue<Integer> queue;
    final QueueInputStream inputQStream;
    final QueueOutputStream outputQStream;

    public BufferedInputStream inputStream;

    private long lastBatchStart;
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardBufferChannelRecordReader.class);
    private final RecordReaderFactory readerFactory;
    private final SocketChannelRecordReaderDispatcher dispatcher;
    private final String remoteAddress;
    private RecordReader recordReader;
    private boolean closed = false;
    private final SocketChannel socketChannel;
    private boolean closing;

    public StandardBufferChannelRecordReader(final SocketChannel socketChannel, RecordReaderFactory readerFactory, SocketChannelRecordReaderDispatcher dispatcher, String remoteAddress, Integer recordBatchSize) throws IOException {
        this.readerFactory = readerFactory;
        this.dispatcher = dispatcher;
        this.remoteAddress = remoteAddress;
        this.lastBatchStart = -1;
        this.socketChannel = socketChannel;
        this.closing = false;

        queue = new ArrayBlockingQueue<>(recordBatchSize);


         inputQStream = QueueInputStream.builder().setBlockingQueue(queue).setTimeout(Duration.ZERO).get();
         outputQStream = inputQStream.newQueueOutputStream();

        /* inputStreams don't give away the remaining bytes to read, unless they are File-based.
           However, some record readers, like ASN.1, base the decision whether to start reading another record
           erronously on available().
           here, we override the available() method and give away the size of the internal BlockingQueue between the
           non-blocking SocketChannelDispatcher-reader thread and the blocking (ASN.1 or whatever)-record reader
           since we have access to the underlying, manually constructet ArrayBlockingQueue.
         */
        inputStream = new BufferedInputStream(inputQStream){
            @Override
            public int available() throws IOException {
                LOGGER.trace("{} avilable in Queue", queue.size());
                if (queue.peek() != null) {
                    return queue.size();
                } else {
                    if (closing) return -1;
                    return 0; // super.available(); // => 0
                }
            }
        };
        //inputStream = new BufferedInputStream(Channels.newInputStream(pipe.source()));

    }

    @Override
    public void initNewBatch() {
        this.lastBatchStart = System.currentTimeMillis();
    }

    @Override
    public long getBatchAge(){
        if (lastBatchStart == -1L) { return -1L;}
        return System.currentTimeMillis() - lastBatchStart;
    }
    @Override
    public QueueOutputStream receiverOutputStream() {
        return outputQStream;
    }

    @Override
    public boolean isIdle() {
        return (queue.peek() == null );
    }

    @Override
    public RecordReader createRecordReader(final ComponentLog logger) throws IOException, MalformedRecordException, SchemaNotFoundException {
        if (recordReader != null) {
            throw new IllegalStateException("Cannot create RecordReader because already created");
        }

        final InputStream in = this.inputStream;
        recordReader = readerFactory.createRecordReader(Collections.emptyMap(), in, -1, logger);
        return recordReader;
    }

    @Override
    public RecordReader getRecordReader() {
        return recordReader;
    }

    @Override
    public SocketAddress getRemoteAddress() {
        return null;
    }

    public void requestClose() {
        closing = true;
    };

    @Override
    public boolean isClosed() {
        if (closing && isIdle()){
            close();
        }
        return closed;
    }

    @Override
    public String getRemoteAddressString() {
        return this.remoteAddress;
    }

    @Override
    public void close() {
        this.closed = true;
        if (queue.size() > 0) {
            LOGGER.trace("Queue with {} bytes from {} prematurely closed", queue.size(), this.remoteAddress);
            queue.clear();
        }
        IOUtils.closeQuietly(recordReader);
        IOUtils.closeQuietly(socketChannel);
        dispatcher.connectionCompleted();
    }

    @Override
    public int writeAck(ByteBuffer answer) throws IOException {
        return this.socketChannel.write(answer);
    }
}
