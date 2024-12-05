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

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannel;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.serialization.RecordReaderFactory;

/**
 * Accepts connections on the given ServerSocketChannel and dispatches a SocketChannelRecordReader for processing.
 */
public class SocketChannelRecordReaderDispatcher implements Runnable, Closeable {

    private final ServerSocketChannel serverSocketChannel;
    private final SSLContext sslContext;
    private final ClientAuth clientAuth;
    private final int socketReadTimeout;
    private final int receiveBufferSize;
    private final int maxConnections;
    private final RecordReaderFactory readerFactory;
    private final BlockingQueue<BufferedChannelRecordReader> recordReaders;
    private final ComponentLog logger;

    private final AtomicInteger currentConnections = new AtomicInteger(0);
    private Selector selector = null;
    private volatile boolean stopped = false;
    private Map<String,SocketChannelAckWriter> ackWriters;

    public SocketChannelRecordReaderDispatcher(final ServerSocketChannel serverSocketChannel,
                                               final SSLContext sslContext,
                                               final ClientAuth clientAuth,
                                               final int socketReadTimeout,
                                               final int receiveBufferSize,
                                               final int maxConnections,
                                               final RecordReaderFactory readerFactory,
                                               final BlockingQueue<BufferedChannelRecordReader> recordReaders,
                                               final Map<String,SocketChannelAckWriter> ackWriters,
                                               final ComponentLog logger
    ) {
        this.serverSocketChannel = serverSocketChannel;
        this.sslContext = sslContext;
        this.clientAuth = clientAuth;
        this.socketReadTimeout = socketReadTimeout;
        this.receiveBufferSize = receiveBufferSize;
        this.maxConnections = maxConnections;
        this.readerFactory = readerFactory;
        this.recordReaders = recordReaders;
        this.logger = logger;
        this.ackWriters = ackWriters;
    }

    @Override
    public void run() {

        try {
            selector = Selector.open();


            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

        while(!stopped) {
                selector.select(); //  calling selector.select() blocks the current thread until one of the watched channels becomes operation-ready.
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> iter = selectedKeys.iterator();
                while (iter.hasNext()) {

                    SelectionKey key = iter.next();

                    if (key.isAcceptable()) {
                        register(selector, serverSocketChannel);
                    }

                    if (key.isReadable()) {
                        BufferedChannelRecordReader recordReader = (BufferedChannelRecordReader) key.attachment();
                        if (recordReader == null){
                            key.cancel();
                        } else {
                            if (sslContext != null) {
                                SSLBufferChannelRecordReader sslRecordReader = (SSLBufferChannelRecordReader) key.attachment();
                                logger.trace("Ready to read SSL from {}", new Object[]{recordReader.getRemoteAddressString()});
                                try {
                                    pipeSSL(sslRecordReader);
                                } catch (IOException e) {
                                    logger.debug("Failed to pipe SSL data from {} to Reader: {}", new Object[]{recordReader.getRemoteAddressString(), e.getMessage()});
                                }
                        } else {
                                logger.trace("Ready to read from {}", new Object[]{recordReader.getRemoteAddressString()});
                                try {
                                    pipe(recordReader, key);
                                } catch (IOException e) {
                                    logger.debug("Failed to pipe data from {} to Reader: {}", new Object[]{recordReader.getRemoteAddressString(), e.getMessage()});
                                }
                            }
                        }
                    }
                    iter.remove();
                }
        }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                selector.keys().stream().forEach(key -> {
                    try {
                        this.logger.trace("Closing selector {}", new Object[]{key.attachment()});
                        BufferedChannelRecordReader recordReader = (BufferedChannelRecordReader) key.attachment();
                        key.channel().close();
                        key.cancel();
                        if (recordReader != null){
                            recordReader.close();
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                selector.close();
            } catch (IOException e) {
                this.logger.warn("Failed to close selector upon stop. Msg: {}", e);
            }
        }
    }
    private synchronized void pipe(BufferedChannelRecordReader channelRecordReader, SelectionKey key) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        SocketChannel client = (SocketChannel) key.channel();
        int r = 0;
        do{
            r = client.read(buffer);
            if (r == -1) {
                client.close();
                channelRecordReader.requestClose();
                logger.trace("Closing selector {}", new Object[]{channelRecordReader.getRemoteAddressString()});
                logger.trace("Request reader to also close after last message");
            } else {
                buffer.flip();
                byte[] b = new byte[r];
                buffer.get(b);
                channelRecordReader.receiverOutputStream().write(b);
                buffer.clear();
                logger.trace("Piped {} bytes", r); // , receiver is {} Idle",r, channelRecordReader.isIdle() ? "" : "not");
            }
        } while (r>0);
    }
    private synchronized void pipeSSL(SSLBufferChannelRecordReader channelRecordReader) throws IOException {
        byte[] buffer = new byte[1024];
        SSLSocketChannel client = channelRecordReader.getSSLSocketChannel();
        int r = 0;
        do{
            r = client.read(buffer);
            if (r == -1) {
                client.close();
                channelRecordReader.requestClose();
                logger.trace("Closing selector {}", new Object[]{channelRecordReader.getRemoteAddressString()});
                logger.trace("Request reader to also close after last message");
            } else if (r > 0) {
                channelRecordReader.receiverOutputStream().write(buffer,0,r);
                logger.trace("Piped {} bytes", r); // , receiver is {} Idle",r, channelRecordReader.isIdle() ? "" : "not");
            }
        } while (r>0);
    }

    private synchronized void register(Selector selector, ServerSocketChannel serverSocketChannel){
        try {
            final SocketChannel socketChannel = serverSocketChannel.accept();
            // if this channel is in non-blocking mode then this method will immediately return null if there are no pending connections.
            // The socket channel returned by this method, if any, will be in blocking mode regardless of the blocking mode of this channel
            if (socketChannel == null) {
                return;
            }
            socketChannel.configureBlocking(false);
            final SocketAddress remoteSocketAddress = socketChannel.getRemoteAddress();

            socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);

            socketChannel.socket().setSoTimeout(socketReadTimeout);
            socketChannel.socket().setReceiveBufferSize(receiveBufferSize);


            if (logger.isDebugEnabled()) {
                final String remoteAddress = remoteSocketAddress == null ? "null" : remoteSocketAddress.toString();
                logger.trace("Accepted connection from {}", new Object[]{remoteAddress});
            }

            // create a StandardSocketChannelRecordReader or an SSLSocketChannelRecordReader based on presence of SSLContext
            final BufferedChannelRecordReader socketChannelRecordReader;
            if (sslContext == null) {
                socketChannelRecordReader = new StandardBufferChannelRecordReader(socketChannel, readerFactory, this, remoteSocketAddress.toString(), receiveBufferSize); // #TODO: place batch Size here, not ReceiveBuffer
            } else {

                /* TODO: Reimplement Buffered/Piped */
                final SSLEngine sslEngine = sslContext.createSSLEngine();
                sslEngine.setUseClientMode(false);

                switch (clientAuth) {
                    case REQUIRED:
                        sslEngine.setNeedClientAuth(true);
                        break;
                    case WANT:
                        sslEngine.setWantClientAuth(true);
                        break;
                    case NONE:
                        sslEngine.setNeedClientAuth(false);
                        sslEngine.setWantClientAuth(false);
                        break;
                }

                final SSLSocketChannel sslSocketChannel = new SSLSocketChannel(sslEngine, socketChannel);
                socketChannelRecordReader = new SSLBufferChannelRecordReader(socketChannel, sslSocketChannel, readerFactory, this,  remoteSocketAddress.toString(), receiveBufferSize);


            }
            socketChannel.register(selector, SelectionKey.OP_READ,socketChannelRecordReader);

            // queue the SocketChannelRecordReader for processing by the processor
            recordReaders.offer(socketChannelRecordReader);
            if (remoteSocketAddress != null) {
                 ackWriters.put(remoteSocketAddress.toString(), socketChannelRecordReader);
            } else {
                logger.warn("Accepted connection, but No remote socket address provided. No Keepalive responses possible");
            }

        } catch (Exception e) {
            logger.error("Error dispatching connection: " + e.getMessage(), e);
        }
    }

    public int getPort() {
        return serverSocketChannel == null ? 0 : serverSocketChannel.socket().getLocalPort();
    }

    @Override
    public void close() {
        this.stopped = true;
        selector.wakeup();
        try {
            this.serverSocketChannel.close();
            logger.trace("Closed server socket channel");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public void connectionCompleted() {
        currentConnections.decrementAndGet();
    }

}
