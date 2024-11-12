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

import org.apache.nifi.controller.queue.FlowFileQueue;
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
    private final BlockingQueue<SocketChannelRecordReader> recordReaders;
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
                                               final BlockingQueue<SocketChannelRecordReader> recordReaders,
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
                        logger.trace("Ready to read from {}", new Object[]{recordReader.getRemoteAddressString()});
                        pipe(recordReader, key);
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
                        key.channel().close();
                        key.cancel();

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                selector.close();
            } catch (IOException e) {
                this.logger.warn("Failed to close selector. Msg: {}", e);
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
            }
            buffer.flip();
            channelRecordReader.receiverChannel().write(buffer);
            buffer.clear();
            logger.trace("Piped {} bytes, receiver is {} Idle",r, channelRecordReader.isIdle() ? "" : "not");

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
                logger.debug("Accepted connection from {}", new Object[]{remoteAddress});
            }

            // create a StandardSocketChannelRecordReader or an SSLSocketChannelRecordReader based on presence of SSLContext
            final BufferedChannelRecordReader socketChannelRecordReader;
            if (sslContext == null) {
                socketChannelRecordReader = new StandardBufferChannelRecordReader(socketChannel, readerFactory, this, remoteSocketAddress.toString());
            } else {
                socketChannelRecordReader = null;
                /* TODO: Reimplement Buffered/Piped
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
                socketChannelRecordReader = new SSLSocketChannelRecordReader(socketChannel, sslSocketChannel, readerFactory, this, sslEngine);

                 */
            }
            socketChannel.register(selector, SelectionKey.OP_READ,socketChannelRecordReader);

            // queue the SocketChannelRecordReader for processing by the processor
            recordReaders.offer(socketChannelRecordReader);
            if (remoteSocketAddress != null) {
                // FIXME: remove ackwriter after close()
                // ackWriters.put(remoteSocketAddress.toString(), socketChannelRecordReader.getWriter());
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
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public void connectionCompleted() {
        currentConnections.decrementAndGet();
    }

}
