package com.swisscom.nifi.record.listen;

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
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;

public class StandardBufferChannelRecordReader implements BufferedChannelRecordReader{
    java.nio.channels.Pipe pipe;
    Pipe.SinkChannel sinkChannel;
    BufferedInputStream inputStream;

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardSocketChannelRecordReader.class);
    private final RecordReaderFactory readerFactory;
    private final SocketChannelRecordReaderDispatcher dispatcher;
    private final SocketChannelAckWriter ackWriter;
    private final String remoteAddress;
    private RecordReader recordReader;

    public StandardBufferChannelRecordReader(final SocketChannel socketChannel, RecordReaderFactory readerFactory, SocketChannelRecordReaderDispatcher dispatcher, String remoteAddress) throws IOException {
        this.readerFactory = readerFactory;
        this.dispatcher = dispatcher;
        this.ackWriter = null;
        this.remoteAddress = remoteAddress;
        pipe = Pipe.open();
        sinkChannel = pipe.sink();
        inputStream = new BufferedInputStream(Channels.newInputStream(pipe.source()));

    }

    @Override
    public WritableByteChannel receiverChannel() {
        return sinkChannel;
    }

    @Override
    public boolean isIdle() {
        try {
            inputStream.mark(2);
            if (inputStream.read() < 0){
                return true;
            } else {
                inputStream.reset();
                return false;
            }
        } catch (IOException e) {
            throw new RuntimeException("while checking buffer condition:" + e);
        }
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

    @Override
    public SocketChannelAckWriter getWriter() {
        return null;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public String getRemoteAddressString() {
        return this.remoteAddress;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(recordReader);
        dispatcher.connectionCompleted();
    }
}
