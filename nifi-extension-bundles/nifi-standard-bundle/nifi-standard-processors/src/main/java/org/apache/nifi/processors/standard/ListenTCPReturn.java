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
package org.apache.nifi.processors.standard;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.DatagramPacketEncoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.event.transport.EventException;
import org.apache.nifi.event.transport.EventServer;
import org.apache.nifi.event.transport.configuration.BufferAllocator;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.message.ByteArrayMessage;
import org.apache.nifi.event.transport.netty.FilteringStrategy;
import org.apache.nifi.event.transport.netty.NettyEventServerFactory;
import org.apache.nifi.event.transport.netty.channel.ByteArrayMessageChannelHandler;
import org.apache.nifi.event.transport.netty.channel.FilteringByteArrayMessageChannelHandler;
import org.apache.nifi.event.transport.netty.channel.LogExceptionChannelHandler;
import org.apache.nifi.event.transport.netty.codec.DatagramByteArrayMessageDecoder;
import org.apache.nifi.event.transport.netty.codec.SocketByteArrayMessageDecoder;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.listen.EventBatcher;
import org.apache.nifi.processor.util.listen.FlowFileEventBatch;
import org.apache.nifi.processor.util.listen.ListenerProperties;
import org.apache.nifi.processor.util.listen.queue.TrackingLinkedBlockingQueue;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.ssl.SSLContextProvider;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import io.netty.channel.ChannelHandlerContext;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({"listen", "tcp", "tls", "ssl"})
@CapabilityDescription("Listens for incoming TCP connections and reads data from each connection using a line separator " +
        "as the message demarcator. The default behavior is for each message to produce a single FlowFile, however this can " +
        "be controlled by increasing the Batch Size to a larger value for higher throughput. The Receive Buffer Size must be " +
        "set as large as the largest messages expected to be received, meaning if every 100kb there is a line separator, then " +
        "the Receive Buffer Size must be greater than 100kb. " +
        "The processor can be configured to use an SSL Context Service to only allow secure connections. " +
        "When connected clients present certificates for mutual TLS authentication, the Distinguished Names of the client certificate's " +
        "issuer and subject are added to the outgoing FlowFiles as attributes. " +
        "The processor does not perform authorization based on Distinguished Name values, but since these values " +
        "are attached to the outgoing FlowFiles, authorization can be implemented based on these attributes.")
@WritesAttributes({
        @WritesAttribute(attribute = "tcp.sender", description = "The sending host of the messages."),
        @WritesAttribute(attribute = "tcp.port", description = "The sending port the messages were received."),
        @WritesAttribute(attribute = "client.certificate.issuer.dn", description = "For connections using mutual TLS, the Distinguished Name of the " +
                "Certificate Authority that issued the client's certificate " +
                "is attached to the FlowFile."),
        @WritesAttribute(attribute = "client.certificate.subject.dn", description = "For connections using mutual TLS, the Distinguished Name of the " +
                "client certificate's owner (subject) is attached to the FlowFile.")
})
public class ListenTCPReturn extends ListenTCP {

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, " +
                    "messages will be received over a secure connection.")
            .required(false)
            .identifiesControllerService(SSLContextProvider.class)
            .build();

    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("Client Auth")
            .description("The client authentication policy to use for the SSL Context. Only used if an SSL Context Service is provided.")
            .required(true)
            .allowableValues(ClientAuth.values())
            .defaultValue(ClientAuth.REQUIRED.name())
            .dependsOn(SSL_CONTEXT_SERVICE)
            .build();

    protected static final PropertyDescriptor POOL_RECV_BUFFERS = new PropertyDescriptor.Builder()
            .name("pool-receive-buffers")
            .displayName("Pool Receive Buffers")
            .description("Enable or disable pooling of buffers that the processor uses for handling bytes received on socket connections. The framework allocates buffers as needed during processing.")
            .required(true)
            .defaultValue("True")
            .allowableValues("True", "False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    protected static final PropertyDescriptor IDLE_CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("idle-timeout")
            .displayName("Idle Connection Timeout")
            .description("The amount of time a client's connection will remain open if no data is received. The default of 0 seconds will leave connections open until they are closed by the client.")
            .required(true)
            .defaultValue("0 seconds")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ListenerProperties.NETWORK_INTF_NAME,
            ListenerProperties.PORT,
            ListenerProperties.RECV_BUFFER_SIZE,
            ListenerProperties.MAX_MESSAGE_QUEUE_SIZE,
            ListenerProperties.MAX_SOCKET_BUFFER_SIZE,
            ListenerProperties.CHARSET,
            ListenerProperties.WORKER_THREADS,
            ListenerProperties.MAX_BATCH_SIZE,
            ListenerProperties.MESSAGE_DELIMITER,
            IDLE_CONNECTION_TIMEOUT,
            POOL_RECV_BUFFERS,
            SSL_CONTEXT_SERVICE,
            CLIENT_AUTH
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Messages received successfully will be sent out this relationship.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS
    );

    private static final long TRACKING_LOG_INTERVAL = 60000;
    private final AtomicLong nextTrackingLog = new AtomicLong();
    private int eventsCapacity;

    protected volatile int port;
    protected volatile TrackingLinkedBlockingQueue<ByteArrayMessage> events;
    protected volatile BlockingQueue<ByteArrayMessage> errorEvents;
    protected volatile EventServer eventServer;
    protected volatile byte[] messageDemarcatorBytes;
    protected volatile EventBatcher<ByteArrayMessage> eventBatcher;
    protected volatile KeepaliveResponseHandler reponse_channel_handlers;
    @Override
    public void migrateProperties(PropertyConfiguration config) {
        config.removeProperty("max-receiving-threads");
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) throws IOException {
        int workerThreads = context.getProperty(ListenerProperties.WORKER_THREADS).asInteger();
        int bufferSize = context.getProperty(ListenerProperties.RECV_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        int socketBufferSize = context.getProperty(ListenerProperties.MAX_SOCKET_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        Duration idleTimeout = Duration.ofSeconds(context.getProperty(IDLE_CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.SECONDS));
        final String networkInterface = context.getProperty(ListenerProperties.NETWORK_INTF_NAME).evaluateAttributeExpressions().getValue();
        final InetAddress address = NetworkUtils.getInterfaceAddress(networkInterface);
        final Charset charset = Charset.forName(context.getProperty(ListenerProperties.CHARSET).getValue());
        port = context.getProperty(ListenerProperties.PORT).evaluateAttributeExpressions().asInteger();
        eventsCapacity = context.getProperty(ListenerProperties.MAX_MESSAGE_QUEUE_SIZE).asInteger();
        events = new TrackingLinkedBlockingQueue<>(eventsCapacity);
        errorEvents = new LinkedBlockingQueue<>();
        reponse_channel_handlers = new KeepaliveResponseHandler();
    //    final NettyEventServerFactory eventFactory = new ByteArrayMessageNettyEventServerFactory(getLogger(), address, port, TransportProtocol.TCP, messageDemarcatorBytes, bufferSize, events);
        final NettyEventServerFactory eventFactory = new TLVMessagesNettyEventServerFactory(getLogger(), address, port, TransportProtocol.TCP, reponse_channel_handlers, events, FilteringStrategy.DISABLED);

        final SSLContextProvider sslContextProvider = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class);
        if (sslContextProvider != null) {
            final String clientAuthValue = context.getProperty(CLIENT_AUTH).getValue();
            ClientAuth clientAuth = ClientAuth.valueOf(clientAuthValue);
            SSLContext sslContext = sslContextProvider.createContext();
            eventFactory.setSslContext(sslContext);
            eventFactory.setClientAuth(clientAuth);
        }

        final boolean poolReceiveBuffers = context.getProperty(POOL_RECV_BUFFERS).asBoolean();
        final BufferAllocator bufferAllocator = poolReceiveBuffers ? BufferAllocator.POOLED : BufferAllocator.UNPOOLED;
        eventFactory.setBufferAllocator(bufferAllocator);
        eventFactory.setIdleTimeout(idleTimeout);
        eventFactory.setSocketReceiveBuffer(socketBufferSize);
        eventFactory.setWorkerThreads(workerThreads);
        eventFactory.setThreadNamePrefix(String.format("%s[%s]", getClass().getSimpleName(), getIdentifier()));

        try {
            eventServer = eventFactory.getEventServer();
        } catch (EventException e) {
            getLogger().error("Failed to bind to [{}:{}]", address, port, e);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // First treat any incoming Flowfile and look for matching conection by inspecting "remoteaddress" Attribute
        // TODO: Set this attribute on received FFs
        if (context.hasIncomingConnection()){
            FlowFile ff = session.get();
            if (ff != null){
                String remoteaddress = ff.getAttribute("remoteaddress").toString();
                if (remoteaddress.equals("")){
                    getLogger().trace("Flowfile had no attribute <remoteaddress>, discarded");
                    session.remove(ff);
                } else {
                    try {
                        final ChannelHandlerContext ctx = reponse_channel_handlers.lookup(remoteaddress);
                        session.read(ff, inputStream -> {
                            ByteBuf bb = Unpooled.copiedBuffer(inputStream.readAllBytes());
                            ctx.channel().write(bb);
                        });
                    } catch (ChannelNotFoundException e) {
                        getLogger().info("No network channel found for remoteaddress %s, discarded", remoteaddress);
                        session.remove(ff);
                    }
                }
            }
        }

        super.onTrigger(context,session);
    }

    @Override
    protected Map<String, String> getAttributes(FlowFileEventBatch<ByteArrayMessage> batch) {
        final Map<String, String> attributes = new HashMap<>(4);
        attributes.putAll(super.getAttributes(batch));
        if (attributes.getOrDefault("tcp.sender","localhost").contains(":")){
            attributes.put("remoteaddress", attributes.get("tcp.sender"));
        } else {
            getLogger().warn("remote Address %s seems not to contains a port number", attributes.get("tcp.sender"));
        }
        return attributes;
    }

    public class ChannelNotFoundException extends IOException{

        public ChannelNotFoundException(String msg) {
            super(msg);
        }
    };

    @ChannelHandler.Sharable
    private class KeepaliveResponseHandler extends ChannelDuplexHandler {

        private ChannelHandlerContext ctx;
        private java.util.HashMap<String,ChannelHandlerContext> ctxmap = new HashMap<>();


        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            ctxmap.put(ctx.channel().remoteAddress().toString(),ctx);
            super.channelRegistered(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            super.channelUnregistered(ctx);
            ctxmap.remove(ctx.channel().remoteAddress().toString());
        }

        public ChannelHandlerContext lookup(String remoteAddress) throws ChannelNotFoundException {
            ChannelHandlerContext ctx = ctxmap.get(remoteAddress);
            if (ctx == null) throw new ChannelNotFoundException(String.format("Channel %s not found",remoteAddress));
            return ctx;
        }

    }
    protected class AnnotatePortMessageDecoder extends MessageToMessageDecoder<ByteArrayMessage> {
        // the original org.apache.nifi.event.transport.netty.codec.SocketByteArrayMessageDecoder forgets to add the port
        // so that finding the return part from a single message or a batch of messages is impossible
        @Override
        protected void decode(ChannelHandlerContext channelHandlerContext, ByteArrayMessage msg, List<Object> out) throws Exception {
            final InetSocketAddress remoteAddress = (InetSocketAddress) channelHandlerContext.channel().remoteAddress();
            final String address = String.format("%s:%d",remoteAddress.getHostString(), remoteAddress.getPort());
            final ByteArrayMessage message = new ByteArrayMessage(msg.getMessage(), address, msg.getSslSessionStatus());
            out.add(msg);
        }
    }

    private class TLVMessagesNettyEventServerFactory extends NettyEventServerFactory {
        public TLVMessagesNettyEventServerFactory(final ComponentLog log,
                                                  final InetAddress address,
                                                  final int port,
                                                  final TransportProtocol protocol,
                                                  final KeepaliveResponseHandler responseHandler,
                                                  final BlockingQueue<ByteArrayMessage> messages,
                                                  final FilteringStrategy filteringStrategy) {
            super(address, port, protocol);
            final LogExceptionChannelHandler logExceptionChannelHandler = new LogExceptionChannelHandler(log);

            final ByteArrayMessageChannelHandler byteArrayMessageChannelHandler;
            if (FilteringStrategy.EMPTY == filteringStrategy) {
                byteArrayMessageChannelHandler = new FilteringByteArrayMessageChannelHandler(messages){

                };
            } else {
                byteArrayMessageChannelHandler = new ByteArrayMessageChannelHandler(messages){};
            }

            /* setHandlerSupplier translates then to here:
                     handlerSupplier.get().forEach(pipeline::addLast);
               into a standard Netty Pipeline.
               See also https://netty.io/4.1/api/io/netty/channel/ChannelPipeline.html
             */
            // The ChannelPipeline is already pre-populated with e.g. SSLHandler, if needed, in ServerSslHandlerChannelInitializer,
            // when the initial bootstrap is set up by superclass org.apache.nifi.event.transport.netty.NettyEventServerFactory:218
            if (TransportProtocol.UDP.equals(protocol)) {
                setHandlerSupplier(() -> Arrays.asList(
                        new DatagramByteArrayMessageDecoder(),new ByteArrayEncoder(),
                        new AnnotatePortMessageDecoder(),
                        byteArrayMessageChannelHandler,
                        logExceptionChannelHandler
                ));
            } else {
                setHandlerSupplier(() -> Arrays.asList(
                        // Replaces the DelimiterBasedFrameDecoder in the org.apache.nifi.event.transport.netty.ByteArrayMessageNettyEventServerFactory
                        // used by original ListenTCP
                        new LengthFieldBasedFrameDecoder(65532,0,2,-2,0),
                        new ByteArrayDecoder(),new ByteArrayEncoder(),
                        new SocketByteArrayMessageDecoder(),
                        new AnnotatePortMessageDecoder(),
                        byteArrayMessageChannelHandler,
                        responseHandler,
                        logExceptionChannelHandler
                ));

            }
        }
    }
/***
    public int getListeningPort() {
        return eventServer == null ? 0 : eventServer.getListeningPort();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        processTrackingLog();
        final int batchSize = context.getProperty(ListenerProperties.MAX_BATCH_SIZE).asInteger();
        Map<String, FlowFileEventBatch<ByteArrayMessage>> batches = getEventBatcher().getBatches(session, batchSize, messageDemarcatorBytes);
        processEvents(session, batches);
    }

    private void processEvents(final ProcessSession session, final Map<String, FlowFileEventBatch<ByteArrayMessage>> batches) {
        for (Map.Entry<String, FlowFileEventBatch<ByteArrayMessage>> entry : batches.entrySet()) {
            FlowFile flowFile = entry.getValue().getFlowFile();
            final List<ByteArrayMessage> events = entry.getValue().getEvents();

            if (flowFile.getSize() == 0L || events.isEmpty()) {
                session.remove(flowFile);
                getLogger().debug("No data written to FlowFile from batch {}; removing FlowFile", entry.getKey());
                continue;
            }

            final Map<String, String> attributes = getAttributes(entry.getValue());
            addClientCertificateAttributes(attributes, events.getFirst());
            flowFile = session.putAllAttributes(flowFile, attributes);

            getLogger().debug("Transferring {} to success", flowFile);
            session.transfer(flowFile, REL_SUCCESS);
            session.adjustCounter("FlowFiles Transferred to Success", 1L, false);

            final String transitUri = getTransitUri(entry.getValue());
            session.getProvenanceReporter().receive(flowFile, transitUri);
        }
    }

    @OnStopped
    public void stopped() {
        if (eventServer != null) {
            eventServer.shutdown();
        }
        eventBatcher = null;
    }

    protected Map<String, String> getAttributes(final FlowFileEventBatch<ByteArrayMessage> batch) {
        final List<ByteArrayMessage> events = batch.getEvents();
        final String sender = events.getFirst().getSender();
        final Map<String, String> attributes = new HashMap<>(3);
        attributes.put("tcp.sender", sender);
        attributes.put("tcp.port", String.valueOf(port));
        return attributes;
    }

    protected String getTransitUri(final FlowFileEventBatch<ByteArrayMessage> batch) {
        final List<ByteArrayMessage> events = batch.getEvents();
        final String sender = events.getFirst().getSender();
        final String senderHost = sender.startsWith("/") && sender.length() > 1 ? sender.substring(1) : sender;
        return String.format("tcp://%s:%d", senderHost, port);
    }

    @Override
    public final Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    private String getMessageDemarcator(final ProcessContext context) {
        return context.getProperty(ListenerProperties.MESSAGE_DELIMITER)
                .getValue()
                .replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
    }

    private EventBatcher<ByteArrayMessage> getEventBatcher() {
        if (eventBatcher == null) {
            eventBatcher = new EventBatcher<>(getLogger(), events, errorEvents) {
                @Override
                protected String getBatchKey(ByteArrayMessage event) {
                    return event.getSender();
                }
            };
        }
        return eventBatcher;
    }

    private void addClientCertificateAttributes(final Map<String, String> attributes, final ByteArrayMessage event) {
        final SslSessionStatus sslSessionStatus = event.getSslSessionStatus();
        if (sslSessionStatus != null) {
            attributes.put(CLIENT_CERTIFICATE_SUBJECT_DN_ATTRIBUTE, sslSessionStatus.getSubject().getName());
            attributes.put(CLIENT_CERTIFICATE_ISSUER_DN_ATTRIBUTE, sslSessionStatus.getIssuer().getName());
        }
    }

    private void processTrackingLog() {
        final long now = Instant.now().toEpochMilli();
        if (now > nextTrackingLog.get()) {
            getLogger().debug("Event Queue Capacity [{}] Remaining [{}] Size [{}] Largest Size [{}]",
                    eventsCapacity,
                    events.remainingCapacity(),
                    events.size(),
                    events.getLargestSize()
            );
            final long nextTrackingLogScheduled = now + TRACKING_LOG_INTERVAL;
            nextTrackingLog.getAndSet(nextTrackingLogScheduled);
        }
    }
    */
}