package com.kafkademo.order.messaging;

import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.order.order.OrderSagaService;
import java.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class SagaReplyHandler {

    private static final Logger log = LoggerFactory.getLogger(SagaReplyHandler.class);

    private final ProcessedMessageRepository processedMessages;
    private final OrderSagaService saga;
    private final Clock clock;

    public SagaReplyHandler(ProcessedMessageRepository processedMessages, OrderSagaService saga, Clock clock) {
        this.processedMessages = processedMessages;
        this.saga = saga;
        this.clock = clock;
    }

    /**
     * Handles one reply in ONE transaction: the duplicate check and the order change are committed together,
     * or not at all. A copy delivered again later finds its messageId and is skipped.
     */
    @Transactional
    public void handle(MessageEnvelope envelope) {
        if (processedMessages.existsById(envelope.messageId())) {
            log.info("Duplicate message {} ({}) for order {} - already processed, skipped",
                    envelope.messageId(), envelope.type(), envelope.orderId());
            return;
        }
        processedMessages.save(new ProcessedMessage(envelope.messageId(), clock.instant()));
        switch (envelope.type()) {
            case MessageTypes.STOCK_RESERVED -> saga.onStockReserved(envelope.orderId());
            case MessageTypes.STOCK_RESERVATION_FAILED -> saga.onStockReservationFailed(envelope.orderId());
            default -> log.warn("Ignoring unknown reply type '{}' (messageId={})", envelope.type(), envelope.messageId());
        }
    }
}
