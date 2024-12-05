package com.swisscom.nifi.record.listen;

import org.apache.commons.io.input.QueueInputStream;
import org.apache.commons.io.output.QueueOutputStream;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannel;
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
import java.nio.channels.Pipe;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class SSLBufferChannelRecordReader extends StandardBufferChannelRecordReader implements BufferedChannelRecordReader, SocketChannelAckWriter

{

    private static final Logger LOGGER = LoggerFactory.getLogger(SSLBufferChannelRecordReader.class);
    private final SSLSocketChannel sslSocketChannel;

    public SSLBufferChannelRecordReader(final SocketChannel socketChannel, SSLSocketChannel sslSocketChannel, RecordReaderFactory readerFactory, SocketChannelRecordReaderDispatcher dispatcher, String remoteAddress, Integer recordBatchSize) throws IOException {
        super(socketChannel, readerFactory, dispatcher, remoteAddress, recordBatchSize);
        this.sslSocketChannel = sslSocketChannel;
    }

    public SSLSocketChannel getSSLSocketChannel() {
        return sslSocketChannel;
    }


    @Override
    public void close() {
        if (queue.size() > 0) {
            int sslSocketSize = -1;
            try {
                sslSocketSize = this.sslSocketChannel.available();
            } catch (IOException e) {
                LOGGER.debug("Failed to read remaining bytes in SSL Socket Channel while closing: {}. Ignoring", e);
            }
            LOGGER.trace("SSLData with {} bytes and Queue with {} bytes from {} prematurely closed", sslSocketSize, queue.size(), this.getRemoteAddressString());
            queue.clear();
        }
        IOUtils.closeQuietly(sslSocketChannel);
        super.close();
    }

    @Override
    public int writeAck(ByteBuffer answer) throws IOException {
        int bytesToWrite = answer.remaining();
        this.sslSocketChannel.write(answer.array());
        // if no IOException was thrown -> write is supposed to have succeeded
        return bytesToWrite;
    }
}
