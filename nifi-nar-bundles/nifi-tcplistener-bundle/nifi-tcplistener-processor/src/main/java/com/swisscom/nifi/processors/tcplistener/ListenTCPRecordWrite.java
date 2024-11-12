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
package com.swisscom.nifi.processors.tcplistener;

import com.swisscom.nifi.record.listen.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.listen.ListenerProperties;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StopWatch;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.io.OutputStream;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.InterruptedByTimeoutException;
import java.nio.channels.ServerSocketChannel;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nifi.processor.util.listen.ListenerProperties.NETWORK_INTF_NAME;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@TriggerWhenEmpty
@Tags({"listen", "tcp", "record", "tls", "ssl"})
@CapabilityDescription("Listens for incoming TCP connections and reads data from each connection using a configured record " +
        "reader, and writes the records to a flow file using a configured record writer. The type of record reader selected will " +
        "determine how clients are expected to send data. For example, when using a Grok reader to read logs, a client can keep an " +
        "open connection and continuously stream data, but when using an JSON reader, the client cannot send an array of JSON " +
        "documents and then send another array on the same connection, as the reader would be in a bad state at that point. Records " +
        "will be read from the connection in blocking mode, and will timeout according to the Read Timeout specified in the processor. " +
        "If the read times out, or if any other error is encountered when reading, the connection will be closed, and any records " +
        "read up to that point will be handled according to the configured Read Error Strategy (Discard or Transfer). In cases where " +
        "clients are keeping a connection open, the concurrent tasks for the processor should be adjusted to match the Max Number of " +
        "TCP Connections allowed, so that there is a task processing each connection. " +
        "The processor can be configured to use an SSL Context Service to only allow secure connections. " +
        "When connected clients present certificates for mutual TLS authentication, the Distinguished Names of the client certificate's " +
        "issuer and subject are added to the outgoing FlowFiles as attributes. " +
        "The processor does not perform authorization based on Distinguished Name values, but since these values " +
        "are attached to the outgoing FlowFiles, authorization can be implemented based on these attributes.")
@WritesAttributes({
        @WritesAttribute(attribute="tcp.sender", description="The host that sent the data."),
        @WritesAttribute(attribute="tcp.port", description="The port that the processor accepted the connection on."),
        @WritesAttribute(attribute="record.count", description="The number of records written to the flow file."),
        @WritesAttribute(attribute="mime.type", description="The mime-type of the writer used to write the records to the flow file."),
        @WritesAttribute(attribute="client.certificate.issuer.dn", description="For connections using mutual TLS, the Distinguished Name of the " +
                                                                               "Certificate Authority that issued the client's certificate " +
                                                                               "is attached to the FlowFile."),
        @WritesAttribute(attribute="client.certificate.subject.dn", description="For connections using mutual TLS, the Distinguished Name of the " +
                                                                                "client certificate's owner (subject) is attached to the FlowFile.")
})
public class ListenTCPRecordWrite extends AbstractProcessor {
    private static final String CLIENT_CERTIFICATE_SUBJECT_DN_ATTRIBUTE = "client.certificate.subject.dn";
    private static final String CLIENT_CERTIFICATE_ISSUER_DN_ATTRIBUTE = "client.certificate.issuer.dn";

    static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("port")
            .displayName("Port")
            .description("The port to listen on for communication.")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY) // .ENVIRONMENT in 2.0.0
            .build();

    static final PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("read-timeout")
            .displayName("Read Timeout")
            .description("The amount of time to wait before timing out when reading from a connection.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 seconds")
            .required(true)
            .build();

    static final PropertyDescriptor MAX_SOCKET_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("max-size-socket-buffer")
            .displayName("Max Size of Socket Buffer")
            .description("The maximum size of the socket buffer that should be used. This is a suggestion to the Operating System " +
                    "to indicate how big the socket buffer should be. If this value is set too low, the buffer may fill up before " +
                    "the data can be read, and incoming data will be dropped.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .required(true)
            .build();

    static final PropertyDescriptor MAX_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("max-number-tcp-connections")
            .displayName("Max Number of TCP Connections")
            .description("The maximum number of concurrent TCP connections to accept. In cases where clients are keeping a connection open, " +
                    "the concurrent tasks for the processor should be adjusted to match the Max Number of TCP Connections allowed, so that there " +
                    "is a task processing each connection.")
            .addValidator(StandardValidators.createLongValidator(1, 65535, true))
            .defaultValue("2")
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("The Record Reader to use for incoming FlowFiles")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("The Record Writer to use in order to serialize the data before writing to a FlowFile")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final AllowableValue ERROR_HANDLING_DISCARD = new AllowableValue("Discard", "Discard", "Discards any records already received and closes the connection.");
    static final AllowableValue ERROR_HANDLING_TRANSFER = new AllowableValue("Transfer", "Transfer", "Transfers any records already received and closes the connection.");

    static final PropertyDescriptor READER_ERROR_HANDLING_STRATEGY = new PropertyDescriptor.Builder()
            .name("reader-error-handling-strategy")
            .displayName("Read Error Strategy")
            .description("Indicates how to deal with an error while reading the next record from a connection, when previous records have already been read from the connection.")
            .required(true)
            .allowableValues(ERROR_HANDLING_TRANSFER, ERROR_HANDLING_DISCARD)
            .defaultValue(ERROR_HANDLING_TRANSFER.getValue())
            .build();

    static final PropertyDescriptor RECORD_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("record-batch-size")
            .displayName("Record Batch Size")
            .description("The maximum number of records to write to a single FlowFile.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .required(true)
            .build();

    static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl-context-service")
            .displayName("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, " +
                    "messages will be received over a secure connection.")
            .required(false)
            .identifiesControllerService(RestrictedSSLContextService.class)
            .build();

    static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("client-auth")
            .displayName("Client Auth")
            .description("The client authentication policy to use for the SSL Context. Only used if an SSL Context Service is provided.")
            .required(false)
            .allowableValues(ClientAuth.values())
            .defaultValue(ClientAuth.REQUIRED.name())
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Messages received successfully will be sent out this relationship.")
            .build();

    static final Relationship REL_FAILED_ANSWERS = new Relationship.Builder()
            .name("failed answers")
            .description("Messages that can't be sent to the corresponding socket will be sent out this relationship.")
            .build();

    static final List<PropertyDescriptor> PROPERTIES;
    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ListenerProperties.NETWORK_INTF_NAME);
        props.add(PORT);
        props.add(MAX_SOCKET_BUFFER_SIZE);
        props.add(MAX_CONNECTIONS);
        props.add(READ_TIMEOUT);
        props.add(RECORD_READER);
        props.add(RECORD_WRITER);
        props.add(READER_ERROR_HANDLING_STRATEGY);
        props.add(RECORD_BATCH_SIZE);
        props.add(SSL_CONTEXT_SERVICE);
        props.add(CLIENT_AUTH);
        PROPERTIES = Collections.unmodifiableList(props);
    }

    static final Set<Relationship> RELATIONSHIPS;
    static {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILED_ANSWERS);
        RELATIONSHIPS = Collections.unmodifiableSet(rels);
    }

    static final int POLL_TIMEOUT_MS = 20;

    private volatile int port;
    private volatile SocketChannelRecordReaderDispatcher dispatcher;
    // TODO: AckWriter

    private final BlockingQueue<SocketChannelRecordReader> socketReaders = new LinkedBlockingQueue<>();
    private final Map<String, SocketChannelAckWriter> ackWriters = new ConcurrentHashMap<>();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final String clientAuth = validationContext.getProperty(CLIENT_AUTH).getValue();
        final SSLContextService sslContextService = validationContext.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        if (sslContextService != null && StringUtils.isBlank(clientAuth)) {
            results.add(new ValidationResult.Builder()
                    .explanation("Client Auth must be provided when using TLS/SSL")
                    .valid(false).subject("Client Auth").build());
        }

        return results;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        this.port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();

        final int readTimeout = context.getProperty(READ_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final int maxSocketBufferSize = context.getProperty(MAX_SOCKET_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final int maxConnections = context.getProperty(MAX_CONNECTIONS).asInteger();
        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);

        // if the Network Interface Property wasn't provided then a null InetAddress will indicate to bind to all interfaces
        final InetAddress nicAddress;
        final String nicAddressStr = context.getProperty(NETWORK_INTF_NAME).evaluateAttributeExpressions().getValue();
        if (!StringUtils.isEmpty(nicAddressStr)) {
            NetworkInterface netIF = NetworkInterface.getByName(nicAddressStr);
            nicAddress = netIF.getInetAddresses().nextElement();
        } else {
            nicAddress = null;
        }

        SSLContext sslContext = null;
        ClientAuth clientAuth = null;
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslContextService != null) {
            final String clientAuthValue = context.getProperty(CLIENT_AUTH).getValue();
            sslContext = sslContextService.createContext();
            clientAuth = ClientAuth.valueOf(clientAuthValue);
        }

        // create a ServerSocketChannel in non-blocking mode and bind to the given address and port
        final ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        // Rebind to a Socket that is waiting for the remote side to terminate the previous connection
        serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, Boolean.TRUE);
        serverSocketChannel.bind(new InetSocketAddress(nicAddress, port));

        this.dispatcher = new SocketChannelRecordReaderDispatcher(serverSocketChannel, sslContext, clientAuth, readTimeout,
                maxSocketBufferSize, maxConnections, recordReaderFactory, socketReaders, ackWriters, getLogger());

        // start a thread to run the dispatcher
        final Thread readerThread = new Thread(dispatcher);
        readerThread.setName(getClass().getName() + " [" + getIdentifier() + "]");
        readerThread.setDaemon(true);
        readerThread.start();
    }

    public int getListeningPort() {
        return dispatcher.getPort();
    }

    @OnUnscheduled
    public void onUnscheduled(final ProcessContext context) {
        getLogger().trace("Processor {} unscheduled", context.getName());
        if (this.dispatcher != null) {
            this.dispatcher.close(); // implies closing serverSocketChannel
            getLogger().trace("Dispatcher closed");
            this.dispatcher = null;  // dispatcher object and readerThread live on until last run completed and all socketReaders
        } else {
            getLogger().warn("Processor {} unscheduled, but no dispatcher found", context.getName());
        }

    }

    @OnStopped
    public void onStopped() {
        getLogger().trace("Processor stopping");
        SocketChannelRecordReader socketRecordReader;
        while ((socketRecordReader = socketReaders.poll()) != null) {
            try {
                getLogger().trace("Socket Reader {} closing:",socketRecordReader.getRemoteAddressString());
                socketRecordReader.close();
            } catch (Exception e) {
                getLogger().error("Couldn't close " + socketRecordReader, e);
            }
        }
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile inFlowFile = session.get();
        if (inFlowFile != null && inFlowFile.getAttribute("tcp.sender") != null && inFlowFile.getAttribute("tcp.sender") != "null") {
            String senderChannel = (String) inFlowFile.getAttribute("tcp.sender");
            final SocketChannelAckWriter ackWriter;
                ackWriter = this.findAckWriter(senderChannel);
                if (ackWriter == null) {
                    getLogger().debug("sender channel " + senderChannel + " not found in " +
                            ackWriters.keySet().stream().collect(Collectors.joining(",","[","]")));
                }

            if (ackWriter == null) {
                inFlowFile = session.putAttribute(inFlowFile, "failure.reason", "No open socketChannel found for sender");
                session.transfer(inFlowFile, REL_FAILED_ANSWERS);
            } else {
                final StopWatch stopWatch = new StopWatch(true);
                final StringBuilder errStr = new StringBuilder();
                getLogger().info(String.format("Sending FF ({} bytes) to {}", inFlowFile.getSize() ,inFlowFile.getAttribute("tcp.sender")));
                try {
                    session.read(inFlowFile, inputStream -> {
                        try {
                            ackWriter.writeAck(ByteBuffer.wrap(inputStream.readAllBytes()));
                        } catch (java.nio.channels.ClosedChannelException e) {
                            getLogger().debug("Sending Ack failed bcs {}", e.getMessage());
                            errStr.append(e.getMessage());
                        }
                    });
                    if (errStr.length() > 0) { throw new IOException(errStr.toString());}
                    session.getProvenanceReporter().send(inFlowFile, senderChannel, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                    session.remove(inFlowFile);
                    session.commitAsync();
                } catch (final Exception e) {
                    getLogger().error("Send Failed {}", inFlowFile, e);
                    inFlowFile = session.putAttribute(inFlowFile, "failure.reason", e.getMessage());
                    session.transfer(session.penalize(inFlowFile), REL_FAILED_ANSWERS);
                    session.commitAsync();
                    context.yield();
                }
            }
            return;
        }else if (inFlowFile != null ) {
            getLogger().warn("Input Flowfile is missing \"tcp.sender\" attribute, cannot be matched to a TCP socket. Ignoring");
            session.remove(inFlowFile);
            return;
        }


        final SocketChannelRecordReader socketRecordReader = pollForSocketRecordReader();
        if (socketRecordReader == null) {
            return;
        }

        if (socketRecordReader.isClosed()) {
            getLogger().debug("Unable to read records from {}, socket already closed", new Object[] {getRemoteAddress(socketRecordReader)});
            IOUtils.closeQuietly(socketRecordReader); // still need to call close so the overall count is decremented
            return;
            // returning without offering the sockeReader back to the pool. #TODO: move this to after last read
        }
        // Check if Pipe is empty, then return
        // #TODO: move into standard signature  / switch all reader to buffered
        if (((BufferedChannelRecordReader)socketRecordReader).isIdle()){
            socketReaders.offer(socketRecordReader);
            return;
        } else {
            getLogger().trace("able to read records from {}, buffer not empty", new Object[] {getRemoteAddress(socketRecordReader)});
        }

        final int recordBatchSize = context.getProperty(RECORD_BATCH_SIZE).asInteger();
        final String readerErrorHandling = context.getProperty(READER_ERROR_HANDLING_STRATEGY).getValue();
        final RecordSetWriterFactory recordSetWriterFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        // synchronize to ensure there are no stale values in the underlying SocketChannel
        synchronized (socketRecordReader) {
            FlowFile flowFile = session.create();
            try {
                // lazily creating the record reader here
                RecordReader recordReader = socketRecordReader.getRecordReader();
                if (recordReader == null) {
                    recordReader = socketRecordReader.createRecordReader(getLogger());
                }
                // Read the first record, mainly to determine the Schema
                Record record;
                try {
                    getLogger().trace("Try blocking read of first record from: {}", socketRecordReader.getRemoteAddressString());
                    record = recordReader.nextRecord();
                } catch (final Exception e) {
                    boolean timeout = false;
                    getLogger().debug("Caught Exception {} with cause {} while reading first record", e.getClass().getName(), e.getCause().getClass().getName());
                    // some of the underlying record libraries wrap the real exception in RuntimeException, so check each
                    // throwable (starting with the current one) to see if its a SocketTimeoutException
                    Throwable cause = e;
                    while (cause != null) {
                        if (cause instanceof SocketTimeoutException || cause instanceof InterruptedByTimeoutException) {
                            timeout = true;
                            getLogger().debug("Caught SocketTimeoutException {} " + cause, cause.getClass().getName());
                            break;
                        }
                        cause = cause.getCause();
                    }

                    if (timeout) {
                        getLogger().trace("Timeout reading records from {}, will try again later: Msg {} ", socketRecordReader.getRemoteAddressString(),e.getCause().getMessage());
                        socketReaders.offer(socketRecordReader);
                        session.remove(flowFile);
                        return;
                    } else {
                        getLogger().debug("Could not find cause of Exception {}", e.getMessage());
                        throw e;
                    }
                }

                 if (record == null) {
                     // #TODO: This check will fail with NIO, re-implement so that Listener set Close Flag upon receiving FIN
                     if (socketRecordReader.isClosed()){
                        getLogger().trace("No more records available from {}, closing connection", new Object[]{getRemoteAddress(socketRecordReader)});
                        IOUtils.closeQuietly(socketRecordReader);
                    } else {
                         socketReaders.offer(socketRecordReader);
                    }
                    session.remove(flowFile);
                    return;
                }

                String mimeType = null;
                WriteResult writeResult = null;

                final RecordSchema recordSchema = recordSetWriterFactory.getSchema(Collections.EMPTY_MAP, record.getSchema());
                try (final OutputStream out = session.write(flowFile);
                     final RecordSetWriter recordWriter = recordSetWriterFactory.createWriter(getLogger(), recordSchema, out, flowFile)) {

                    // start the record set and write the first record from above
                    recordWriter.beginRecordSet();
                    writeResult = recordWriter.write(record);

                    while (record != null && writeResult.getRecordCount() < recordBatchSize) {
                        // handle a read failure according to the strategy selected...
                        // if discarding then bounce to the outer catch block which will close the connection and remove the flow file
                        // if keeping then null out the record to break out of the loop, which will transfer what we have and close the connection

                        try {
                            record = recordReader.nextRecord();
                        } catch (final SocketTimeoutException ste) {
                            getLogger().trace("Timeout reading records from {}, will try again later",socketRecordReader.getRemoteAddressString());
                            socketReaders.offer(socketRecordReader);
                            session.remove(flowFile);
                            return;
                        } catch (final Exception e) {
                            if (ERROR_HANDLING_DISCARD.getValue().equals(readerErrorHandling)) {
                                throw e;
                            } else {
                                record = null;
                            }
                        }

                    }

                    writeResult = recordWriter.finishRecordSet();
                    recordWriter.flush();
                    mimeType = recordWriter.getMimeType();
                }

                // if we didn't write any records then we need to remove the flow file
                if (writeResult.getRecordCount() <= 0) {
                    getLogger().debug("Removing flow file, no records were written");
                    session.remove(flowFile);
                } else {
                    final String sender = getRemoteAddress(socketRecordReader);

                    final Map<String, String> attributes = new HashMap<>(writeResult.getAttributes());
                    attributes.put(CoreAttributes.MIME_TYPE.key(), mimeType);
                    attributes.put("tcp.sender", sender);
                    attributes.put("tcp.port", String.valueOf(port));
                    attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                    addClientCertificateAttributes(attributes, socketRecordReader);
                    flowFile = session.putAllAttributes(flowFile, attributes);

                    final String senderHost = sender.startsWith("/") && sender.length() > 1 ? sender.substring(1) : sender;
                    final String transitUri = new StringBuilder().append("tcp").append("://").append(senderHost).append(":").append(port).toString();
                    session.getProvenanceReporter().receive(flowFile, transitUri);

                    session.transfer(flowFile, REL_SUCCESS);
                }

                getLogger().trace("Re-queuing connection {} for further processing...", socketRecordReader.getRemoteAddressString());
                socketReaders.offer(socketRecordReader);

            } catch (Exception e) {
                getLogger().error("Error processing records: " + e.getMessage(), e);
                IOUtils.closeQuietly(socketRecordReader);
                session.remove(flowFile);
                return;
            }
        }
    }

    private SocketChannelRecordReader pollForSocketRecordReader() {
        try {
            return socketReaders.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private synchronized SocketChannelAckWriter findAckWriter(String remoteAddress) {
        try {
            SocketChannelAckWriter result = ackWriters.get(remoteAddress);
        } catch (NullPointerException e) {
            getLogger().debug("Socket {} does not exist:" + e.getMessage(), remoteAddress);
            return null;
        }
        getLogger().debug("Socket {} not found anymore", remoteAddress);
        return null;
    }

    private String getRemoteAddress(final SocketChannelRecordReader socketChannelRecordReader) {
        return socketChannelRecordReader.getRemoteAddressString() == null ? "null" : socketChannelRecordReader.getRemoteAddressString();
    }

    private void addClientCertificateAttributes(final Map<String, String> attributes, final SocketChannelRecordReader socketRecordReader)
            throws SSLPeerUnverifiedException {
        if (socketRecordReader instanceof SSLSocketChannelRecordReader) {
            SSLSocketChannelRecordReader sslSocketRecordReader = (SSLSocketChannelRecordReader) socketRecordReader;
            SSLSession sslSession = sslSocketRecordReader.getSession();
            try {
                Certificate[] certificates = sslSession.getPeerCertificates();
                if (certificates.length > 0) {
                    X509Certificate certificate = (X509Certificate) certificates[0];
                    attributes.put(CLIENT_CERTIFICATE_SUBJECT_DN_ATTRIBUTE, certificate.getSubjectDN().toString());
                    attributes.put(CLIENT_CERTIFICATE_ISSUER_DN_ATTRIBUTE, certificate.getIssuerDN().toString());
                }
            } catch (SSLPeerUnverifiedException peerUnverifiedException) {
                getLogger().debug("Remote Peer [{}] not verified: client certificates not provided",
                        socketRecordReader.getRemoteAddress(), peerUnverifiedException);
            }
        }
    }
}
