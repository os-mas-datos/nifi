/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.swisscom.nifi.record.listen;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannel;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannelInputStream;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Collections;

/**
 * Encapsulates an SSLSocketChannel and a RecordReader created for the given channel.
 */
public class SSLSocketChannelRecordReader implements SocketChannelRecordReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(SSLSocketChannel.class);
    private final SocketChannel socketChannel;
    private final SSLSocketChannel sslSocketChannel;
    private final RecordReaderFactory readerFactory;
    private final SocketChannelRecordReaderDispatcher dispatcher;
    private final SSLEngine sslEngine;
    private final SocketChannelAckWriter ackWriter;
    private final String remoteAddress;
    private RecordReader recordReader;

    public SSLSocketChannelRecordReader(final SocketChannel socketChannel,
                                        final SSLSocketChannel sslSocketChannel,
                                        final RecordReaderFactory readerFactory,
                                        final SocketChannelRecordReaderDispatcher dispatcher,
                                        final SSLEngine sslEngine) {
        this.socketChannel = socketChannel;
        this.sslSocketChannel = sslSocketChannel;
        this.readerFactory = readerFactory;
        this.dispatcher = dispatcher;
        this.sslEngine = sslEngine;
        this.ackWriter = new SSLSocketChannelAckWriter(sslSocketChannel);
        String remoteAddress1;
        try {
            remoteAddress1 = socketChannel.getRemoteAddress().toString();
        } catch (IOException e) {
            LOGGER.warn("RemoteAddress can't be determined: {}", e.getMessage().toString());
            remoteAddress1 = "";
        }
        this.remoteAddress = remoteAddress1;
    }

    @Override
    public RecordReader createRecordReader(final ComponentLog logger) throws IOException, MalformedRecordException, SchemaNotFoundException {
        if (recordReader != null) {
            throw new IllegalStateException("Cannot create RecordReader because already created");
        }

        final InputStream socketIn = new SSLSocketChannelInputStream(sslSocketChannel);
        // final BlockingQueue<Byte>
        final InputStream in = new BufferedInputStream(socketIn);
        recordReader = readerFactory.createRecordReader(Collections.emptyMap(), in, -1, logger);
        return recordReader;
    }

    @Override
    public RecordReader getRecordReader() {
        return recordReader;
    }

    @Override
    public SocketAddress getRemoteAddress() {
        return socketChannel.socket().getRemoteSocketAddress();
    }

    @Override
    public SocketChannelAckWriter getWriter() {
        return ackWriter;
    }

    @Override
    public boolean isClosed() {
        return sslSocketChannel.isClosed();
    }

    @Override
    public String getRemoteAddressString() {
        return this.remoteAddress;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(recordReader);
        IOUtils.closeQuietly(sslSocketChannel);
        dispatcher.connectionCompleted();
    }

    public SSLSession getSession() {
        return sslEngine.getSession();
    }
}
