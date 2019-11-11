/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.PulsarClientException.CryptoException;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.ProtocolVersion;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.shaded.com.google.protobuf.v241.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.google.common.base.Preconditions.checkArgument;
import static com.scurrilous.circe.checksum.Crc32cIntChecksum.computeChecksum;
import static com.scurrilous.circe.checksum.Crc32cIntChecksum.resumeChecksum;
import static java.lang.String.format;
import static org.apache.pulsar.common.protocol.Commands.hasChecksum;
import static org.apache.pulsar.common.protocol.Commands.readChecksum;

public class ProducerImpl<T> extends ProducerBase<T> implements TimerTask, ConnectionHandler.Connection {

    // Producer id, used to identify a producer within a single connection
    protected final long producerId;

    // Variable is used through the atomic updater
    private volatile long msgIdGenerator;

    private final BlockingQueue<OpSendMsg> pendingMessages;
    private final BlockingQueue<OpSendMsg> pendingCallbacks;
    private final Semaphore semaphore;
    private volatile Timeout sendTimeout = null;
    private volatile Timeout batchMessageAndSendTimeout = null;
    private final long createProducerTimeout;
    private final int maxNumMessagesInBatch;
    private final BatchMessageContainerBase batchMessageContainer;
    private CompletableFuture<MessageId> lastSendFuture = CompletableFuture.completedFuture(null);

    // Globally unique producer name
    private String producerName;

    private String connectionId;
    private String connectedSince;
    private final int partitionIndex;

    private final ProducerStatsRecorder stats;

    private final CompressionCodec compressor;

    private volatile long lastSequenceIdPublished;
    private MessageCrypto msgCrypto = null;

    private ScheduledFuture<?> keyGeneratorTask = null;

    private final Map<String, String> metadata;

    private Optional<byte[]> schemaVersion = Optional.empty();

    private final ConnectionHandler connectionHandler;

    private static final AtomicLongFieldUpdater<ProducerImpl> msgIdGeneratorUpdater = AtomicLongFieldUpdater
            .newUpdater(ProducerImpl.class, "msgIdGenerator");

    public ProducerImpl(final PulsarClientImpl client, final String topic, final ProducerConfigurationData conf,
                        final CompletableFuture<Producer<T>> producerCreatedFuture, final int partitionIndex, final Schema<T> schema,
                        final ProducerInterceptors<T> interceptors) {
        super(client, topic, conf, producerCreatedFuture, schema, interceptors);
        this.producerId = client.newProducerId();
        this.producerName = conf.getProducerName();
        this.partitionIndex = partitionIndex;
        this.pendingMessages = Queues.newArrayBlockingQueue(conf.getMaxPendingMessages());
        this.pendingCallbacks = Queues.newArrayBlockingQueue(conf.getMaxPendingMessages());
        this.semaphore = new Semaphore(conf.getMaxPendingMessages(), true);

        this.compressor = CompressionCodecProvider.getCompressionCodec(conf.getCompressionType());

        if (conf.getInitialSequenceId() != null) {
            final long initialSequenceId = conf.getInitialSequenceId();
            this.lastSequenceIdPublished = initialSequenceId;
            this.msgIdGenerator = initialSequenceId + 1;
        } else {
            this.lastSequenceIdPublished = -1;
            this.msgIdGenerator = 0;
        }

        if (conf.isEncryptionEnabled()) {
            final String logCtx = "[" + topic + "] [" + this.producerName + "] [" + this.producerId + "]";
            this.msgCrypto = new MessageCrypto(logCtx, true);

            // Regenerate data key cipher at fixed interval
            this.keyGeneratorTask = client.eventLoopGroup().scheduleWithFixedDelay(() -> {
                try {
                    this.msgCrypto.addPublicKeyCipher(conf.getEncryptionKeys(), conf.getCryptoKeyReader());
                } catch (final CryptoException e) {
                    if (!producerCreatedFuture.isDone()) {
                        ProducerImpl.log.warn("[{}] [{}] [{}] Failed to add public key cipher.", topic, this.producerName, this.producerId);
                        producerCreatedFuture.completeExceptionally(e);
                    }
                }
            }, 0L, 4L, TimeUnit.HOURS);

        }

        if (conf.getSendTimeoutMs() > 0) {
            this.sendTimeout = client.timer().newTimeout(this, conf.getSendTimeoutMs(), TimeUnit.MILLISECONDS);
        }

        this.createProducerTimeout = System.currentTimeMillis() + client.getConfiguration().getOperationTimeoutMs();
        if (conf.isBatchingEnabled()) {
            this.maxNumMessagesInBatch = conf.getBatchingMaxMessages();
            BatcherBuilder containerBuilder = conf.getBatcherBuilder();
            if (containerBuilder == null) {
                containerBuilder = BatcherBuilder.DEFAULT;
            }
            this.batchMessageContainer = (BatchMessageContainerBase) containerBuilder.build();
            this.batchMessageContainer.setProducer(this);
        } else {
            this.maxNumMessagesInBatch = 1;
            this.batchMessageContainer = null;
        }
        if (client.getConfiguration().getStatsIntervalSeconds() > 0) {
            this.stats = new ProducerStatsRecorderImpl(client, conf, this);
        } else {
            this.stats = ProducerStatsDisabled.INSTANCE;
        }

        if (conf.getProperties().isEmpty()) {
            this.metadata = Collections.emptyMap();
        } else {
            this.metadata = Collections.unmodifiableMap(new HashMap<>(conf.getProperties()));
        }

        this.connectionHandler = new ConnectionHandler(this,
                new BackoffBuilder()
                        .setInitialTime(100, TimeUnit.MILLISECONDS)
                        .setMax(60, TimeUnit.SECONDS)
                        .setMandatoryStop(Math.max(100, conf.getSendTimeoutMs() - 100), TimeUnit.MILLISECONDS)
                        .useUserConfiguredIntervals(client.getConfiguration().getDefaultBackoffIntervalNanos(),
                                client.getConfiguration().getMaxBackoffIntervalNanos())
                        .create(),
                this);

        this.grabCnx();
    }

    public ConnectionHandler getConnectionHandler() {
        return this.connectionHandler;
    }

    private boolean isBatchMessagingEnabled() {
        return this.conf.isBatchingEnabled();
    }

    @Override
    public long getLastSequenceId() {
        return this.lastSequenceIdPublished;
    }

    @Override
    CompletableFuture<MessageId> internalSendAsync(final Message<T> message) {

        final CompletableFuture<MessageId> future = new CompletableFuture<>();

        final MessageImpl<T> interceptorMessage = (MessageImpl<T>) this.beforeSend(message);
        //Retain the buffer used by interceptors callback to get message. Buffer will release after complete interceptors.
        interceptorMessage.getDataBuffer().retain();
        if (this.interceptors != null) {
            interceptorMessage.getProperties();
        }
        this.sendAsync(interceptorMessage, new SendCallback() {
            SendCallback nextCallback = null;
            MessageImpl<?> nextMsg = null;
            final long createdAt = System.nanoTime();

            @Override
            public CompletableFuture<MessageId> getFuture() {
                return future;
            }

            @Override
            public SendCallback getNextSendCallback() {
                return this.nextCallback;
            }

            @Override
            public MessageImpl<?> getNextMessage() {
                return this.nextMsg;
            }

            @Override
            public void sendComplete(final Exception e) {
                try {
                    if (e != null) {
                        ProducerImpl.this.stats.incrementSendFailed();
                        ProducerImpl.this.onSendAcknowledgement(interceptorMessage, null, e);
                        future.completeExceptionally(e);
                    } else {
                        ProducerImpl.this.onSendAcknowledgement(interceptorMessage, interceptorMessage.getMessageId(), null);
                        future.complete(interceptorMessage.getMessageId());
                        ProducerImpl.this.stats.incrementNumAcksReceived(System.nanoTime() - this.createdAt);
                    }
                } finally {
                    interceptorMessage.getDataBuffer().release();
                }

                while (this.nextCallback != null) {
                    final SendCallback sendCallback = this.nextCallback;
                    final MessageImpl<?> msg = this.nextMsg;
                    //Retain the buffer used by interceptors callback to get message. Buffer will release after complete interceptors.
                    try {
                        msg.getDataBuffer().retain();
                        if (e != null) {
                            ProducerImpl.this.stats.incrementSendFailed();
                            ProducerImpl.this.onSendAcknowledgement((Message<T>) msg, null, e);
                            sendCallback.getFuture().completeExceptionally(e);
                        } else {
                            ProducerImpl.this.onSendAcknowledgement((Message<T>) msg, msg.getMessageId(), null);
                            sendCallback.getFuture().complete(msg.getMessageId());
                            ProducerImpl.this.stats.incrementNumAcksReceived(System.nanoTime() - this.createdAt);
                        }
                        this.nextMsg = this.nextCallback.getNextMessage();
                        this.nextCallback = this.nextCallback.getNextSendCallback();
                    } finally {
                        msg.getDataBuffer().release();
                    }
                }
            }

            @Override
            public void addCallback(final MessageImpl<?> msg, final SendCallback scb) {
                this.nextMsg = msg;
                this.nextCallback = scb;
            }
        });
        return future;
    }

    public void sendAsync(final Message<T> message, final SendCallback callback) {
        checkArgument(message instanceof MessageImpl);

        if (!this.isValidProducerState(callback)) {
            return;
        }

        if (!this.canEnqueueRequest(callback)) {
            return;
        }

        final MessageImpl<T> msg = (MessageImpl<T>) message;
        final MessageMetadata.Builder msgMetadataBuilder = msg.getMessageBuilder();
        final ByteBuf payload = msg.getDataBuffer();

        // If compression is enabled, we are compressing, otherwise it will simply use the same buffer
        final int uncompressedSize = payload.readableBytes();
        ByteBuf compressedPayload = payload;
        // Batch will be compressed when closed
        // If a message has a delayed delivery time, we'll always send it individually
        if (!this.isBatchMessagingEnabled() || msgMetadataBuilder.hasDeliverAtTime()) {
            compressedPayload = this.compressor.encode(payload);
            payload.release();

            // validate msg-size (For batching this will be check at the batch completion size)
            final int compressedSize = compressedPayload.readableBytes();
            if (compressedSize > ClientCnx.getMaxMessageSize()) {
                compressedPayload.release();
                final String compressedStr = (!this.isBatchMessagingEnabled() && this.conf.getCompressionType() != CompressionType.NONE)
                        ? "Compressed"
                        : "";
                final PulsarClientException.InvalidMessageException invalidMessageException = new PulsarClientException.InvalidMessageException(
                        format("%s Message payload size %d cannot exceed %d bytes", compressedStr, compressedSize,
                                ClientCnx.getMaxMessageSize()));
                callback.sendComplete(invalidMessageException);
                return;
            }
        }

        if (!msg.isReplicated() && msgMetadataBuilder.hasProducerName()) {
            final PulsarClientException.InvalidMessageException invalidMessageException =
                    new PulsarClientException.InvalidMessageException("Cannot re-use the same message");
            callback.sendComplete(invalidMessageException);
            compressedPayload.release();
            return;
        }

        if (this.schemaVersion.isPresent()) {
            msgMetadataBuilder.setSchemaVersion(ByteString.copyFrom(this.schemaVersion.get()));
        }

        try {
            synchronized (this) {
                final long sequenceId;
                if (!msgMetadataBuilder.hasSequenceId()) {
                    sequenceId = ProducerImpl.msgIdGeneratorUpdater.getAndIncrement(this);
                    msgMetadataBuilder.setSequenceId(sequenceId);
                } else {
                    sequenceId = msgMetadataBuilder.getSequenceId();
                }
                if (!msgMetadataBuilder.hasPublishTime()) {
                    msgMetadataBuilder.setPublishTime(this.client.getClientClock().millis());

                    checkArgument(!msgMetadataBuilder.hasProducerName());

                    msgMetadataBuilder.setProducerName(this.producerName);

                    if (this.conf.getCompressionType() != CompressionType.NONE) {
                        msgMetadataBuilder.setCompression(
                                CompressionCodecProvider.convertToWireProtocol(this.conf.getCompressionType()));
                    }
                    msgMetadataBuilder.setUncompressedSize(uncompressedSize);
                }

                if (this.isBatchMessagingEnabled() && !msgMetadataBuilder.hasDeliverAtTime()) {
                    // handle boundary cases where message being added would exceed
                    // batch size and/or max message size
                    if (this.batchMessageContainer.haveEnoughSpace(msg)) {
                        this.batchMessageContainer.add(msg, callback);
                        this.lastSendFuture = callback.getFuture();
                        payload.release();
                        if (this.batchMessageContainer.getNumMessagesInBatch() == this.maxNumMessagesInBatch
                                || this.batchMessageContainer.getCurrentBatchSize() >= BatchMessageContainerImpl.MAX_MESSAGE_BATCH_SIZE_BYTES) {
                            this.batchMessageAndSend();
                        }
                    } else {
                        this.doBatchSendAndAdd(msg, callback, payload);
                    }
                } else {
                    final ByteBuf encryptedPayload = this.encryptMessage(msgMetadataBuilder, compressedPayload);

                    final MessageMetadata msgMetadata = msgMetadataBuilder.build();

                    // When publishing during replication, we need to set the correct number of message in batch
                    // This is only used in tracking the publish rate stats
                    final int numMessages = msg.getMessageBuilder().hasNumMessagesInBatch()
                            ? msg.getMessageBuilder().getNumMessagesInBatch()
                            : 1;
                    final ByteBufPair cmd = this.sendMessage(this.producerId, sequenceId, numMessages, msgMetadata, encryptedPayload);
                    msgMetadataBuilder.recycle();
                    msgMetadata.recycle();

                    final OpSendMsg op = OpSendMsg.create(msg, cmd, sequenceId, callback);
                    op.setNumMessagesInBatch(numMessages);
                    op.setBatchSizeByte(encryptedPayload.readableBytes());
                    this.pendingMessages.put(op);
                    this.lastSendFuture = callback.getFuture();

                    // Read the connection before validating if it's still connected, so that we avoid reading a null
                    // value
                    final ClientCnx cnx = this.cnx();
                    if (this.isConnected()) {
                        // If we do have a connection, the message is sent immediately, otherwise we'll try again once a
                        // new
                        // connection is established
                        cmd.retain();
                        cnx.ctx().channel().eventLoop().execute(WriteInEventLoopCallback.create(this, cnx, op));
                        this.stats.updateNumMsgsSent(op.numMessagesInBatch, op.batchSizeByte);
                    } else {
                        if (ProducerImpl.log.isDebugEnabled()) {
                            ProducerImpl.log.debug("[{}] [{}] Connection is not ready -- sequenceId {}", this.topic, this.producerName,
                                    sequenceId);
                        }
                    }
                }
            }
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            this.semaphore.release();
            callback.sendComplete(new PulsarClientException(ie));
        } catch (final PulsarClientException e) {
            this.semaphore.release();
            callback.sendComplete(e);
        } catch (final Throwable t) {
            this.semaphore.release();
            callback.sendComplete(new PulsarClientException(t));
        }
    }

    protected ByteBuf encryptMessage(final MessageMetadata.Builder msgMetadata, final ByteBuf compressedPayload)
            throws PulsarClientException {

        ByteBuf encryptedPayload = compressedPayload;
        if (!this.conf.isEncryptionEnabled() || this.msgCrypto == null) {
            return encryptedPayload;
        }
        try {
            encryptedPayload = this.msgCrypto.encrypt(this.conf.getEncryptionKeys(), this.conf.getCryptoKeyReader(), msgMetadata,
                    compressedPayload);
        } catch (final PulsarClientException e) {
            // Unless config is set to explicitly publish un-encrypted message upon failure, fail the request
            if (this.conf.getCryptoFailureAction() == ProducerCryptoFailureAction.SEND) {
                ProducerImpl.log.warn("[{}] [{}] Failed to encrypt message {}. Proceeding with publishing unencrypted message",
                        this.topic, this.producerName, e.getMessage());
                return compressedPayload;
            }
            throw e;
        }
        return encryptedPayload;
    }

    protected ByteBufPair sendMessage(final long producerId, final long sequenceId, final int numMessages, final MessageMetadata msgMetadata,
                                      final ByteBuf compressedPayload) throws IOException {
        final ChecksumType checksumType;

        if (this.connectionHandler.getClientCnx() == null
                || this.connectionHandler.getClientCnx().getRemoteEndpointProtocolVersion() >= this.brokerChecksumSupportedVersion()) {
            checksumType = ChecksumType.Crc32c;
        } else {
            checksumType = ChecksumType.None;
        }
        return Commands.newSend(producerId, sequenceId, numMessages, checksumType, msgMetadata, compressedPayload);
    }

    private void doBatchSendAndAdd(final MessageImpl<T> msg, final SendCallback callback, final ByteBuf payload) {
        if (ProducerImpl.log.isDebugEnabled()) {
            ProducerImpl.log.debug("[{}] [{}] Closing out batch to accommodate large message with size {}", this.topic, this.producerName,
                    msg.getDataBuffer().readableBytes());
        }
        this.batchMessageAndSend();
        this.batchMessageContainer.add(msg, callback);
        this.lastSendFuture = callback.getFuture();
        payload.release();
    }

    private boolean isValidProducerState(final SendCallback callback) {
        switch (this.getState()) {
            case Ready:
                // OK
            case Connecting:
                // We are OK to queue the messages on the client, it will be sent to the broker once we get the connection
                return true;
            case Closing:
            case Closed:
                callback.sendComplete(new PulsarClientException.AlreadyClosedException("Producer already closed"));
                return false;
            case Terminated:
                callback.sendComplete(new PulsarClientException.TopicTerminatedException("Topic was terminated"));
                return false;
            case Failed:
            case Uninitialized:
            default:
                callback.sendComplete(new PulsarClientException.NotConnectedException());
                return false;
        }
    }

    private boolean canEnqueueRequest(final SendCallback callback) {
        try {
            if (this.conf.isBlockIfQueueFull()) {
                this.semaphore.acquire();
            } else {
                if (!this.semaphore.tryAcquire()) {
                    callback.sendComplete(new PulsarClientException.ProducerQueueIsFullError("Producer send queue is full"));
                    return false;
                }
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            callback.sendComplete(new PulsarClientException(e));
            return false;
        }

        return true;
    }

    private static final class WriteInEventLoopCallback implements Runnable {
        private ProducerImpl<?> producer;
        private ByteBufPair cmd;
        private long sequenceId;
        private ClientCnx cnx;

        static WriteInEventLoopCallback create(final ProducerImpl<?> producer, final ClientCnx cnx, final OpSendMsg op) {
            final WriteInEventLoopCallback c = WriteInEventLoopCallback.RECYCLER.get();
            c.producer = producer;
            c.cnx = cnx;
            c.sequenceId = op.sequenceId;
            c.cmd = op.cmd;
            return c;
        }

        @Override
        public void run() {
            if (ProducerImpl.log.isDebugEnabled()) {
                ProducerImpl.log.debug("[{}] [{}] Sending message cnx {}, sequenceId {}", this.producer.topic, this.producer.producerName, this.cnx,
                        this.sequenceId);
            }

            try {
                this.cnx.ctx().writeAndFlush(this.cmd, this.cnx.ctx().voidPromise());
            } finally {
                this.recycle();
            }
        }

        private void recycle() {
            this.producer = null;
            this.cnx = null;
            this.cmd = null;
            this.sequenceId = -1;
            this.recyclerHandle.recycle(this);
        }

        private final Handle<WriteInEventLoopCallback> recyclerHandle;

        private WriteInEventLoopCallback(final Handle<WriteInEventLoopCallback> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<WriteInEventLoopCallback> RECYCLER = new Recycler<WriteInEventLoopCallback>() {
            @Override
            protected WriteInEventLoopCallback newObject(final Handle<WriteInEventLoopCallback> handle) {
                return new WriteInEventLoopCallback(handle);
            }
        };
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        final State currentState = this.getAndUpdateState(state -> {
            if (state == State.Closed) {
                return state;
            }
            return State.Closing;
        });

        if (currentState == State.Closed || currentState == State.Closing) {
            return CompletableFuture.completedFuture(null);
        }

        final Timeout timeout = this.sendTimeout;
        if (timeout != null) {
            timeout.cancel();
            this.sendTimeout = null;
        }

        final Timeout batchTimeout = this.batchMessageAndSendTimeout;
        if (batchTimeout != null) {
            batchTimeout.cancel();
            this.batchMessageAndSendTimeout = null;
        }

        if (this.keyGeneratorTask != null && !this.keyGeneratorTask.isCancelled()) {
            this.keyGeneratorTask.cancel(false);
        }

        this.stats.cancelStatsTimeout();

        final ClientCnx cnx = this.cnx();
        if (cnx == null || currentState != State.Ready) {
            ProducerImpl.log.info("[{}] [{}] Closed Producer (not connected)", this.topic, this.producerName);
            synchronized (this) {
                this.setState(State.Closed);
                this.client.cleanupProducer(this);
                final PulsarClientException ex = new PulsarClientException.AlreadyClosedException(
                        "Producer was already closed");
                this.pendingMessages.forEach(msg -> {
                    msg.callback.sendComplete(ex);
                    msg.cmd.release();
                    msg.recycle();
                });
                this.pendingMessages.clear();
            }

            return CompletableFuture.completedFuture(null);
        }

        final long requestId = this.client.newRequestId();
        final ByteBuf cmd = Commands.newCloseProducer(this.producerId, requestId);

        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        cnx.sendRequestWithId(cmd, requestId).handle((v, exception) -> {
            cnx.removeProducer(this.producerId);
            if (exception == null || !cnx.ctx().channel().isActive()) {
                // Either we've received the success response for the close producer command from the broker, or the
                // connection did break in the meantime. In any case, the producer is gone.
                synchronized (ProducerImpl.this) {
                    ProducerImpl.log.info("[{}] [{}] Closed Producer", this.topic, this.producerName);
                    this.setState(State.Closed);
                    this.pendingMessages.forEach(msg -> {
                        msg.cmd.release();
                        msg.recycle();
                    });
                    this.pendingMessages.clear();
                }

                closeFuture.complete(null);
                this.client.cleanupProducer(this);
            } else {
                closeFuture.completeExceptionally(exception);
            }

            return null;
        });

        return closeFuture;
    }

    @Override
    public boolean isConnected() {
        return this.connectionHandler.getClientCnx() != null && (this.getState() == State.Ready);
    }

    public boolean isWritable() {
        final ClientCnx cnx = this.connectionHandler.getClientCnx();
        return cnx != null && cnx.channel().isWritable();
    }

    public void terminated(final ClientCnx cnx) {
        final State previousState = this.getAndUpdateState(state -> (state == State.Closed ? State.Closed : State.Terminated));
        if (previousState != State.Terminated && previousState != State.Closed) {
            ProducerImpl.log.info("[{}] [{}] The topic has been terminated", this.topic, this.producerName);
            this.setClientCnx(null);

            this.failPendingMessages(cnx,
                    new PulsarClientException.TopicTerminatedException("The topic has been terminated"));
        }
    }

    void ackReceived(final ClientCnx cnx, final long sequenceId, final long ledgerId, final long entryId) {
        OpSendMsg op = null;
        boolean callback = false;
        synchronized (this) {
            op = this.pendingMessages.peek();
            if (op == null) {
                if (ProducerImpl.log.isDebugEnabled()) {
                    ProducerImpl.log.debug("[{}] [{}] Got ack for timed out msg {}", this.topic, this.producerName, sequenceId);
                }
                return;
            }

            final long expectedSequenceId = op.sequenceId;
            if (sequenceId > expectedSequenceId) {
                ProducerImpl.log.warn("[{}] [{}] Got ack for msg. expecting: {} - got: {} - queue-size: {}", this.topic, this.producerName,
                        expectedSequenceId, sequenceId, this.pendingMessages.size());
                // Force connection closing so that messages can be re-transmitted in a new connection
                cnx.channel().close();
            } else if (sequenceId < expectedSequenceId) {
                // Ignoring the ack since it's referring to a message that has already timed out.
                if (ProducerImpl.log.isDebugEnabled()) {
                    ProducerImpl.log.debug("[{}] [{}] Got ack for timed out msg {} last-seq: {}", this.topic, this.producerName, sequenceId,
                            expectedSequenceId);
                }
            } else {
                // Message was persisted correctly
                if (ProducerImpl.log.isDebugEnabled()) {
                    ProducerImpl.log.debug("[{}] [{}] Received ack for msg {} ", this.topic, this.producerName, sequenceId);
                }
                this.pendingMessages.remove();
                this.semaphore.release(this.isBatchMessagingEnabled() ? op.numMessagesInBatch : 1);
                callback = true;
                this.pendingCallbacks.add(op);
            }
        }
        if (callback) {
            op = this.pendingCallbacks.poll();
            if (op != null) {
                this.lastSequenceIdPublished = op.sequenceId + op.numMessagesInBatch - 1;
                op.setMessageId(ledgerId, entryId, this.partitionIndex);
                try {
                    // Need to protect ourselves from any exception being thrown in the future handler from the
                    // application
                    op.callback.sendComplete(null);
                } catch (final Throwable t) {
                    ProducerImpl.log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", this.topic, this.producerName,
                            sequenceId, t);
                }
                ReferenceCountUtil.safeRelease(op.cmd);
                op.recycle();
            }
        }
    }

    /**
     * Checks message checksum to retry if message was corrupted while sending to broker. Recomputes checksum of the
     * message header-payload again.
     * <ul>
     * <li><b>if matches with existing checksum</b>: it means message was corrupt while sending to broker. So, resend
     * message</li>
     * <li><b>if doesn't match with existing checksum</b>: it means message is already corrupt and can't retry again.
     * So, fail send-message by failing callback</li>
     * </ul>
     *
     * @param cnx
     * @param sequenceId
     */
    protected synchronized void recoverChecksumError(final ClientCnx cnx, final long sequenceId) {
        final OpSendMsg op = this.pendingMessages.peek();
        if (op == null) {
            if (ProducerImpl.log.isDebugEnabled()) {
                ProducerImpl.log.debug("[{}] [{}] Got send failure for timed out msg {}", this.topic, this.producerName, sequenceId);
            }
        } else {
            final long expectedSequenceId = op.sequenceId;
            if (sequenceId == expectedSequenceId) {
                final boolean corrupted = !this.verifyLocalBufferIsNotCorrupted(op);
                if (corrupted) {
                    // remove message from pendingMessages queue and fail callback
                    this.pendingMessages.remove();
                    this.semaphore.release(this.isBatchMessagingEnabled() ? op.numMessagesInBatch : 1);
                    try {
                        op.callback.sendComplete(
                                new PulsarClientException.ChecksumException("Checksum failed on corrupt message"));
                    } catch (final Throwable t) {
                        ProducerImpl.log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", this.topic,
                                this.producerName, sequenceId, t);
                    }
                    ReferenceCountUtil.safeRelease(op.cmd);
                    op.recycle();
                    return;
                } else {
                    if (ProducerImpl.log.isDebugEnabled()) {
                        ProducerImpl.log.debug("[{}] [{}] Message is not corrupted, retry send-message with sequenceId {}", this.topic,
                                this.producerName, sequenceId);
                    }
                }

            } else {
                if (ProducerImpl.log.isDebugEnabled()) {
                    ProducerImpl.log.debug("[{}] [{}] Corrupt message is already timed out {}", this.topic, this.producerName, sequenceId);
                }
            }
        }
        // as msg is not corrupted : let producer resend pending-messages again including checksum failed message
        this.resendMessages(cnx);
    }

    /**
     * Computes checksum again and verifies it against existing checksum. If checksum doesn't match it means that
     * message is corrupt.
     *
     * @param op
     * @return returns true only if message is not modified and computed-checksum is same as previous checksum else
     *         return false that means that message is corrupted. Returns true if checksum is not present.
     */
    protected boolean verifyLocalBufferIsNotCorrupted(final OpSendMsg op) {
        final ByteBufPair msg = op.cmd;

        if (msg != null) {
            final ByteBuf headerFrame = msg.getFirst();
            headerFrame.markReaderIndex();
            try {
                // skip bytes up to checksum index
                headerFrame.skipBytes(4); // skip [total-size]
                final int cmdSize = (int) headerFrame.readUnsignedInt();
                headerFrame.skipBytes(cmdSize);
                // verify if checksum present
                if (hasChecksum(headerFrame)) {
                    final int checksum = readChecksum(headerFrame);
                    // msg.readerIndex is already at header-payload index, Recompute checksum for headers-payload
                    final int metadataChecksum = computeChecksum(headerFrame);
                    final long computedChecksum = resumeChecksum(metadataChecksum, msg.getSecond());
                    return checksum == computedChecksum;
                } else {
                    ProducerImpl.log.warn("[{}] [{}] checksum is not present into message with id {}", this.topic, this.producerName,
                            op.sequenceId);
                }
            } finally {
                headerFrame.resetReaderIndex();
            }
            return true;
        } else {
            ProducerImpl.log.warn("[{}] Failed while casting {} into ByteBufPair", this.producerName, op.cmd.getClass().getName());
            return false;
        }
    }

    protected static final class OpSendMsg {
        MessageImpl<?> msg;
        List<MessageImpl<?>> msgs;
        ByteBufPair cmd;
        SendCallback callback;
        long sequenceId;
        long createdAt;
        long batchSizeByte = 0;
        int numMessagesInBatch = 1;

        static OpSendMsg create(final MessageImpl<?> msg, final ByteBufPair cmd, final long sequenceId, final SendCallback callback) {
            final OpSendMsg op = OpSendMsg.RECYCLER.get();
            op.msg = msg;
            op.cmd = cmd;
            op.callback = callback;
            op.sequenceId = sequenceId;
            op.createdAt = System.currentTimeMillis();
            return op;
        }

        static OpSendMsg create(final List<MessageImpl<?>> msgs, final ByteBufPair cmd, final long sequenceId, final SendCallback callback) {
            final OpSendMsg op = OpSendMsg.RECYCLER.get();
            op.msgs = msgs;
            op.cmd = cmd;
            op.callback = callback;
            op.sequenceId = sequenceId;
            op.createdAt = System.currentTimeMillis();
            return op;
        }

        void recycle() {
            this.msg = null;
            this.msgs = null;
            this.cmd = null;
            this.callback = null;
            this.sequenceId = -1;
            this.createdAt = -1;
            this.recyclerHandle.recycle(this);
        }

        void setNumMessagesInBatch(final int numMessagesInBatch) {
            this.numMessagesInBatch = numMessagesInBatch;
        }

        void setBatchSizeByte(final long batchSizeByte) {
            this.batchSizeByte = batchSizeByte;
        }

        void setMessageId(final long ledgerId, final long entryId, final int partitionIndex) {
            if (this.msg != null) {
                this.msg.setMessageId(new MessageIdImpl(ledgerId, entryId, partitionIndex));
            } else {
                for (int batchIndex = 0; batchIndex < this.msgs.size(); batchIndex++) {
                    this.msgs.get(batchIndex)
                            .setMessageId(new BatchMessageIdImpl(ledgerId, entryId, partitionIndex, batchIndex));
                }
            }
        }

        private OpSendMsg(final Handle<OpSendMsg> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private final Handle<OpSendMsg> recyclerHandle;
        private static final Recycler<OpSendMsg> RECYCLER = new Recycler<OpSendMsg>() {
            @Override
            protected OpSendMsg newObject(final Handle<OpSendMsg> handle) {
                return new OpSendMsg(handle);
            }
        };
    }

    @Override
    public void connectionOpened(final ClientCnx cnx) {
        // we set the cnx reference before registering the producer on the cnx, so if the cnx breaks before creating the
        // producer, it will try to grab a new cnx
        this.connectionHandler.setClientCnx(cnx);
        cnx.registerProducer(this.producerId, this);

        ProducerImpl.log.info("[{}] [{}] Creating producer on cnx {}", this.topic, this.producerName, cnx.ctx().channel());

        final long requestId = this.client.newRequestId();

        SchemaInfo schemaInfo = null;
        if (this.schema != null) {
            if (this.schema.getSchemaInfo() != null) {
                if (this.schema.getSchemaInfo().getType() == SchemaType.JSON) {
                    // for backwards compatibility purposes
                    // JSONSchema originally generated a schema for pojo based of of the JSON schema standard
                    // but now we have standardized on every schema to generate an Avro based schema
                    if (Commands.peerSupportJsonSchemaAvroFormat(cnx.getRemoteEndpointProtocolVersion())) {
                        schemaInfo = this.schema.getSchemaInfo();
                    } else if (this.schema instanceof JSONSchema) {
                        final JSONSchema jsonSchema = (JSONSchema) this.schema;
                        schemaInfo = jsonSchema.getBackwardsCompatibleJsonSchemaInfo();
                    } else {
                        schemaInfo = this.schema.getSchemaInfo();
                    }
                } else if (this.schema.getSchemaInfo().getType() == SchemaType.BYTES
                        || this.schema.getSchemaInfo().getType() == SchemaType.NONE) {
                    // don't set schema info for Schema.BYTES
                    schemaInfo = null;
                } else {
                    schemaInfo = this.schema.getSchemaInfo();
                }
            }
        }

        cnx.sendRequestWithId(
                Commands.newProducer(this.topic, this.producerId, requestId, this.producerName, this.conf.isEncryptionEnabled(), this.metadata,
                        schemaInfo),
                requestId).thenAccept(response -> {
            String producerName = response.getProducerName();
            long lastSequenceId = response.getLastSequenceId();
            this.schemaVersion = Optional.ofNullable(response.getSchemaVersion());

            // We are now reconnected to broker and clear to send messages. Re-send all pending messages and
            // set the cnx pointer so that new messages will be sent immediately
            synchronized (ProducerImpl.this) {
                if (this.getState() == State.Closing || this.getState() == State.Closed) {
                    // Producer was closed while reconnecting, close the connection to make sure the broker
                    // drops the producer on its side
                    cnx.removeProducer(this.producerId);
                    cnx.channel().close();
                    return;
                }
                this.resetBackoff();

                ProducerImpl.log.info("[{}] [{}] Created producer on cnx {}", this.topic, producerName, cnx.ctx().channel());
                this.connectionId = cnx.ctx().channel().toString();
                this.connectedSince = DateFormatter.now();

                if (this.producerName == null) {
                    this.producerName = producerName;
                }

                if (this.msgIdGenerator == 0 && this.conf.getInitialSequenceId() == null) {
                    // Only update sequence id generator if it wasn't already modified. That means we only want
                    // to update the id generator the first time the producer gets established, and ignore the
                    // sequence id sent by broker in subsequent producer reconnects
                    this.lastSequenceIdPublished = lastSequenceId;
                    this.msgIdGenerator = lastSequenceId + 1;
                }

                if (!this.producerCreatedFuture.isDone() && this.isBatchMessagingEnabled()) {
                    // schedule the first batch message task
                    this.client.timer().newTimeout(this.batchMessageAndSendTask, this.conf.getBatchingMaxPublishDelayMicros(),
                            TimeUnit.MICROSECONDS);
                }
                this.resendMessages(cnx);
            }
        }).exceptionally((e) -> {
            final Throwable cause = e.getCause();
            cnx.removeProducer(this.producerId);
            if (this.getState() == State.Closing || this.getState() == State.Closed) {
                // Producer was closed while reconnecting, close the connection to make sure the broker
                // drops the producer on its side
                cnx.channel().close();
                return null;
            }
            ProducerImpl.log.error("[{}] [{}] Failed to create producer: {}", this.topic, this.producerName, cause.getMessage());

            if (cause instanceof PulsarClientException.ProducerBlockedQuotaExceededException) {
                synchronized (this) {
                    ProducerImpl.log.warn("[{}] [{}] Topic backlog quota exceeded. Throwing Exception on producer.", this.topic,
                            this.producerName);

                    if (ProducerImpl.log.isDebugEnabled()) {
                        ProducerImpl.log.debug("[{}] [{}] Pending messages: {}", this.topic, this.producerName,
                                this.pendingMessages.size());
                    }

                    final PulsarClientException bqe = new PulsarClientException.ProducerBlockedQuotaExceededException(
                            "Could not send pending messages as backlog exceeded");
                    this.failPendingMessages(this.cnx(), bqe);
                }
            } else if (cause instanceof PulsarClientException.ProducerBlockedQuotaExceededError) {
                ProducerImpl.log.warn("[{}] [{}] Producer is blocked on creation because backlog exceeded on topic.",
                        this.producerName, this.topic);
            }

            if (cause instanceof PulsarClientException.TopicTerminatedException) {
                this.setState(State.Terminated);
                this.failPendingMessages(this.cnx(), (PulsarClientException) cause);
                this.producerCreatedFuture.completeExceptionally(cause);
                this.client.cleanupProducer(this);
            } else if (this.producerCreatedFuture.isDone() || //
                    (cause instanceof PulsarClientException && this.connectionHandler.isRetriableError((PulsarClientException) cause)
                            && System.currentTimeMillis() < this.createProducerTimeout)) {
                // Either we had already created the producer once (producerCreatedFuture.isDone()) or we are
                // still within the initial timeout budget and we are dealing with a retriable error
                this.reconnectLater(cause);
            } else {
                this.setState(State.Failed);
                this.producerCreatedFuture.completeExceptionally(cause);
                this.client.cleanupProducer(this);
            }

            return null;
        });
    }

    @Override
    public void connectionFailed(final PulsarClientException exception) {
        if (System.currentTimeMillis() > this.createProducerTimeout
                && this.producerCreatedFuture.completeExceptionally(exception)) {
            ProducerImpl.log.info("[{}] Producer creation failed for producer {}", this.topic, this.producerId);
            this.setState(State.Failed);
            this.client.cleanupProducer(this);
        }
    }

    private void resendMessages(final ClientCnx cnx) {
        cnx.ctx().channel().eventLoop().execute(() -> {
            synchronized (this) {
                if (this.getState() == State.Closing || this.getState() == State.Closed) {
                    // Producer was closed while reconnecting, close the connection to make sure the broker
                    // drops the producer on its side
                    cnx.channel().close();
                    return;
                }
                final int messagesToResend = this.pendingMessages.size();
                if (messagesToResend == 0) {
                    if (ProducerImpl.log.isDebugEnabled()) {
                        ProducerImpl.log.debug("[{}] [{}] No pending messages to resend {}", this.topic, this.producerName, messagesToResend);
                    }
                    if (this.changeToReadyState()) {
                        this.producerCreatedFuture.complete(ProducerImpl.this);
                        return;
                    } else {
                        // Producer was closed while reconnecting, close the connection to make sure the broker
                        // drops the producer on its side
                        cnx.channel().close();
                        return;
                    }

                }

                ProducerImpl.log.info("[{}] [{}] Re-Sending {} messages to server", this.topic, this.producerName, messagesToResend);

                final boolean stripChecksum = cnx.getRemoteEndpointProtocolVersion() < this.brokerChecksumSupportedVersion();
                for (final OpSendMsg op : this.pendingMessages) {

                    if (stripChecksum) {
                        this.stripChecksum(op);
                    }
                    op.cmd.retain();
                    if (ProducerImpl.log.isDebugEnabled()) {
                        ProducerImpl.log.debug("[{}] [{}] Re-Sending message in cnx {}, sequenceId {}", this.topic, this.producerName,
                                cnx.channel(), op.sequenceId);
                    }
                    cnx.ctx().write(op.cmd, cnx.ctx().voidPromise());
                    this.stats.updateNumMsgsSent(op.numMessagesInBatch, op.batchSizeByte);
                }

                cnx.ctx().flush();
                if (!this.changeToReadyState()) {
                    // Producer was closed while reconnecting, close the connection to make sure the broker
                    // drops the producer on its side
                    cnx.channel().close();
                    return;
                }
            }
        });
    }

    /**
     * Strips checksum from {@link OpSendMsg} command if present else ignore it.
     *
     * @param op
     */
    private void stripChecksum(final OpSendMsg op) {
        final int totalMsgBufSize = op.cmd.readableBytes();
        final ByteBufPair msg = op.cmd;
        if (msg != null) {
            final ByteBuf headerFrame = msg.getFirst();
            headerFrame.markReaderIndex();
            try {
                headerFrame.skipBytes(4); // skip [total-size]
                final int cmdSize = (int) headerFrame.readUnsignedInt();

                // verify if checksum present
                headerFrame.skipBytes(cmdSize);

                if (!hasChecksum(headerFrame)) {
                    return;
                }

                final int headerSize = 4 + 4 + cmdSize; // [total-size] [cmd-length] [cmd-size]
                final int checksumSize = 4 + 2; // [magic-number] [checksum-size]
                final int checksumMark = (headerSize + checksumSize); // [header-size] [checksum-size]
                final int metaPayloadSize = (totalMsgBufSize - checksumMark); // metadataPayload = totalSize - checksumMark
                final int newTotalFrameSizeLength = 4 + cmdSize + metaPayloadSize; // new total-size without checksum
                headerFrame.resetReaderIndex();
                final int headerFrameSize = headerFrame.readableBytes();

                headerFrame.setInt(0, newTotalFrameSizeLength); // rewrite new [total-size]
                final ByteBuf metadata = headerFrame.slice(checksumMark, headerFrameSize - checksumMark); // sliced only
                // metadata
                headerFrame.writerIndex(headerSize); // set headerFrame write-index to overwrite metadata over checksum
                metadata.readBytes(headerFrame, metadata.readableBytes());
                headerFrame.capacity(headerFrameSize - checksumSize); // reduce capacity by removed checksum bytes
            } finally {
                headerFrame.resetReaderIndex();
            }
        } else {
            ProducerImpl.log.warn("[{}] Failed while casting {} into ByteBufPair", this.producerName, op.cmd.getClass().getName());
        }
    }

    public int brokerChecksumSupportedVersion() {
        return ProtocolVersion.v6.getNumber();
    }

    @Override
    String getHandlerName() {
        return this.producerName;
    }

    /**
     * Process sendTimeout events
     */
    @Override
    public void run(final Timeout timeout) throws Exception {
        if (timeout.isCancelled()) {
            return;
        }

        final long timeToWaitMs;

        synchronized (this) {
            // If it's closing/closed we need to ignore this timeout and not schedule next timeout.
            if (this.getState() == State.Closing || this.getState() == State.Closed) {
                return;
            }

            final OpSendMsg firstMsg = this.pendingMessages.peek();
            if (firstMsg == null) {
                // If there are no pending messages, reset the timeout to the configured value.
                timeToWaitMs = this.conf.getSendTimeoutMs();
            } else {
                // If there is at least one message, calculate the diff between the message timeout and the current
                // time.
                final long diff = (firstMsg.createdAt + this.conf.getSendTimeoutMs()) - System.currentTimeMillis();
                if (diff <= 0) {
                    // The diff is less than or equal to zero, meaning that the message has been timed out.
                    // Set the callback to timeout on every message, then clear the pending queue.
                    ProducerImpl.log.info("[{}] [{}] Message send timed out. Failing {} messages", this.topic, this.producerName,
                            this.pendingMessages.size());

                    final PulsarClientException te = new PulsarClientException.TimeoutException(
                            "Could not send message to broker within given timeout");
                    this.failPendingMessages(this.cnx(), te);
                    this.stats.incrementSendFailed(this.pendingMessages.size());
                    // Since the pending queue is cleared now, set timer to expire after configured value.
                    timeToWaitMs = this.conf.getSendTimeoutMs();
                } else {
                    // The diff is greater than zero, set the timeout to the diff value
                    timeToWaitMs = diff;
                }
            }

            this.sendTimeout = this.client.timer().newTimeout(this, timeToWaitMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * This fails and clears the pending messages with the given exception. This method should be called from within the
     * ProducerImpl object mutex.
     */
    private void failPendingMessages(final ClientCnx cnx, final PulsarClientException ex) {
        if (cnx == null) {
            final AtomicInteger releaseCount = new AtomicInteger();
            this.pendingMessages.forEach(op -> {
                releaseCount.addAndGet(this.isBatchMessagingEnabled() ? op.numMessagesInBatch : 1);
                try {
                    // Need to protect ourselves from any exception being thrown in the future handler from the
                    // application
                    op.callback.sendComplete(ex);
                } catch (final Throwable t) {
                    ProducerImpl.log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", this.topic, this.producerName,
                            op.sequenceId, t);
                }
                ReferenceCountUtil.safeRelease(op.cmd);
                op.recycle();
            });

            this.pendingMessages.clear();
            this.pendingCallbacks.clear();
            this.semaphore.release(releaseCount.get());

            if (this.isBatchMessagingEnabled()) {
                this.failPendingBatchMessages(ex);
            }
        } else {
            // If we have a connection, we schedule the callback and recycle on the event loop thread to avoid any
            // race condition since we also write the message on the socket from this thread
            cnx.ctx().channel().eventLoop().execute(() -> {
                synchronized (ProducerImpl.this) {
                    this.failPendingMessages(null, ex);
                }
            });
        }
    }

    /**
     * fail any pending batch messages that were enqueued, however batch was not closed out
     *
     */
    private void failPendingBatchMessages(final PulsarClientException ex) {
        if (this.batchMessageContainer.isEmpty()) {
            return;
        }
        final int numMessagesInBatch = this.batchMessageContainer.getNumMessagesInBatch();
        this.batchMessageContainer.discard(ex);
        this.semaphore.release(numMessagesInBatch);
    }

    TimerTask batchMessageAndSendTask = new TimerTask() {

        @Override
        public void run(final Timeout timeout) throws Exception {
            if (timeout.isCancelled()) {
                return;
            }
            if (ProducerImpl.log.isDebugEnabled()) {
                ProducerImpl.log.debug("[{}] [{}] Batching the messages from the batch container from timer thread", ProducerImpl.this.topic,
                        ProducerImpl.this.producerName);
            }
            // semaphore acquired when message was enqueued to container
            synchronized (ProducerImpl.this) {
                // If it's closing/closed we need to ignore the send batch timer and not schedule next timeout.
                if (ProducerImpl.this.getState() == State.Closing || ProducerImpl.this.getState() == State.Closed) {
                    return;
                }

                ProducerImpl.this.batchMessageAndSend();
                // schedule the next batch message task
                ProducerImpl.this.batchMessageAndSendTimeout = ProducerImpl.this.client.timer()
                        .newTimeout(this, ProducerImpl.this.conf.getBatchingMaxPublishDelayMicros(), TimeUnit.MICROSECONDS);
            }
        }
    };

    @Override
    public CompletableFuture<Void> flushAsync() {
        final CompletableFuture<MessageId> lastSendFuture;
        synchronized (ProducerImpl.this) {
            if (this.isBatchMessagingEnabled()) {
                this.batchMessageAndSend();
            }
            lastSendFuture = this.lastSendFuture;
        }
        return lastSendFuture.thenApply(ignored -> null);
    }

    @Override
    protected void triggerFlush() {
        if (this.isBatchMessagingEnabled()) {
            synchronized (ProducerImpl.this) {
                this.batchMessageAndSend();
            }
        }
    }

    // must acquire semaphore before enqueuing
    private void batchMessageAndSend() {
        if (ProducerImpl.log.isDebugEnabled()) {
            ProducerImpl.log.debug("[{}] [{}] Batching the messages from the batch container with {} messages", this.topic, this.producerName,
                    this.batchMessageContainer.getNumMessagesInBatch());
        }
        if (!this.batchMessageContainer.isEmpty()) {
            try {
                if (this.batchMessageContainer.isMultiBatches()) {
                    final List<OpSendMsg> opSendMsgs = this.batchMessageContainer.createOpSendMsgs();
                    for (final OpSendMsg opSendMsg : opSendMsgs) {
                        this.processOpSendMsg(opSendMsg);
                    }
                } else {
                    final OpSendMsg opSendMsg = this.batchMessageContainer.createOpSendMsg();
                    if (opSendMsg != null) {
                        this.processOpSendMsg(opSendMsg);
                    }
                }
            } catch (final PulsarClientException e) {
                Thread.currentThread().interrupt();
                this.semaphore.release(this.batchMessageContainer.getNumMessagesInBatch());
            } catch (final Throwable t) {
                this.semaphore.release(this.batchMessageContainer.getNumMessagesInBatch());
                ProducerImpl.log.warn("[{}] [{}] error while create opSendMsg by batch message container -- {}", this.topic, this.producerName, t);
            }
        }
    }

    private void processOpSendMsg(final OpSendMsg op) {
        try {
            this.batchMessageContainer.clear();
            this.pendingMessages.put(op);
            final ClientCnx cnx = this.cnx();
            if (this.isConnected()) {
                // If we do have a connection, the message is sent immediately, otherwise we'll try again once a new
                // connection is established
                op.cmd.retain();
                cnx.ctx().channel().eventLoop().execute(WriteInEventLoopCallback.create(this, cnx, op));
                this.stats.updateNumMsgsSent(op.numMessagesInBatch, op.batchSizeByte);
            } else {
                if (ProducerImpl.log.isDebugEnabled()) {
                    ProducerImpl.log.debug("[{}] [{}] Connection is not ready -- sequenceId {}", this.topic, this.producerName,
                            op.sequenceId);
                }
            }
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            this.semaphore.release(this.isBatchMessagingEnabled() ? op.numMessagesInBatch : 1);
            if (op != null) {
                op.callback.sendComplete(new PulsarClientException(ie));
            }
        } catch (final Throwable t) {
            this.semaphore.release(this.isBatchMessagingEnabled() ? op.numMessagesInBatch : 1);
            ProducerImpl.log.warn("[{}] [{}] error while closing out batch -- {}", this.topic, this.producerName, t);
            if (op != null) {
                op.callback.sendComplete(new PulsarClientException(t));
            }
        }
    }

    public long getDelayInMillis() {
        final OpSendMsg firstMsg = this.pendingMessages.peek();
        if (firstMsg != null) {
            return System.currentTimeMillis() - firstMsg.createdAt;
        }
        return 0L;
    }

    public String getConnectionId() {
        return this.cnx() != null ? this.connectionId : null;
    }

    public String getConnectedSince() {
        return this.cnx() != null ? this.connectedSince : null;
    }

    public int getPendingQueueSize() {
        return this.pendingMessages.size();
    }

    @Override
    public ProducerStatsRecorder getStats() {
        return this.stats;
    }

    @Override
    public String getProducerName() {
        return this.producerName;
    }

    // wrapper for connection methods
    ClientCnx cnx() {
        return this.connectionHandler.cnx();
    }

    void resetBackoff() {
        this.connectionHandler.resetBackoff();
    }

    void connectionClosed(final ClientCnx cnx) {
        this.connectionHandler.connectionClosed(cnx);
    }

    ClientCnx getClientCnx() {
        return this.connectionHandler.getClientCnx();
    }

    void setClientCnx(final ClientCnx clientCnx) {
        this.connectionHandler.setClientCnx(clientCnx);
    }

    void reconnectLater(final Throwable exception) {
        this.connectionHandler.reconnectLater(exception);
    }

    void grabCnx() {
        this.connectionHandler.grabCnx();
    }

    @VisibleForTesting
    Semaphore getSemaphore() {
        return this.semaphore;
    }

    private static final Logger log = LoggerFactory.getLogger(ProducerImpl.class);
}
