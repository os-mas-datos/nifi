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
import java.nio.ByteBuffer;
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
    }

    @Override
    public RecordReader createRecordReader(final ComponentLog logger) throws IOException, MalformedRecordException, SchemaNotFoundException {
        if (recordReader != null) {
            throw new IllegalStateException("Cannot create RecordReader because already created");
        }

        final InputStream socketIn = new SSLSocketChannelInputStream(sslSocketChannel);
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
        try {
            LOGGER.debug("getRemoteAddress(socketChannel {} ) = isBlocking {} isConnected {} isOpen {} isConnectionPending {} localAddress {} remoteAddress {}",
                    socketChannel.getClass().toString(),
                    Boolean.toString(socketChannel.isBlocking()),
                    Boolean.toString(socketChannel.isConnected()),
                    Boolean.toString(socketChannel.isOpen()),
                    Boolean.toString(socketChannel.isConnectionPending()),
                    socketChannel.getLocalAddress(),
                    socketChannel.getRemoteAddress()
                    );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        LOGGER.debug("getRemoteAddress(socketChannel.socket {} ) = isBound {} isConnected {} isClosed {} localAddress {} remoteAddress {}",
                socketChannel.socket().getClass().toString(),
                Boolean.toString(socketChannel.socket().isBound()),
                Boolean.toString(socketChannel.socket().isClosed()),
                Boolean.toString(socketChannel.socket().isConnected()),
                socketChannel.socket().getInetAddress().toString(),
                socketChannel.socket().getRemoteSocketAddress().toString()
        );
        /*
        LOGGER.debug("getRemoteAddress(sslSocketChannel {} ) = isClosed {}",
                sslSocketChannel.getClass().toString(),
                Boolean.toString(sslSocketChannel.isClosed()) // this causes a read ! if blocking, this'll time out
                // org/apache/nifi/remote/io/socket/ssl/SSLSocketChannel.java:207
        );

         */
        if (socketChannel.socket().getRemoteSocketAddress() == null ){
            LOGGER.debug("getRemoteAddress == null : Trying to connect socketChannel.socket().");
            try {
                sslSocketChannel.connect();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (socketChannel.socket().getRemoteSocketAddress() == null ){
                LOGGER.debug("getRemoteAddress is still null");
            }
        }
        return socketChannel.socket().getRemoteSocketAddress();
    }
    // @TODO
    private String remoteAddressString = "unset";
    public String getRemoteAddressString() {
        if (! remoteAddressString.equals(getRemoteAddress().toString())){
            LOGGER.warn("getRemoteAddressString {} unlike reported value: {}", remoteAddressString, getRemoteAddress().toString());
        }
        return remoteAddressString;
    }

    public void setRemoteAddressString(String remoteAddressString) {
        this.remoteAddressString
                = remoteAddressString;
    }

    @Override
    public int writeAck(ByteBuffer answer) throws IOException {
        int len = answer.array().length;
        this.sslSocketChannel.write(answer.array());
        return len;
    }

    @Override
    public boolean isClosed() {
        return sslSocketChannel.isClosed();
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
