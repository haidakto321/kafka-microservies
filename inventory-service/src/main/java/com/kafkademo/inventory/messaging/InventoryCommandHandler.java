package com.kafkademo.inventory.messaging;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.ReserveStock;
import com.kafkademo.contracts.payload.StockReservationFailed;
import com.kafkademo.contracts.payload.StockReserved;
import com.kafkademo.inventory.outbox.OutboxWriter;
import com.kafkademo.inventory.reservation.ReservationResult;
import com.kafkademo.inventory.reservation.StockService;
import java.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InventoryCommandHandler {

    private static final Logger log = LoggerFactory.getLogger(InventoryCommandHandler.class);

    private final ProcessedMessageRepository processedMessages;
    private final StockService stockService;
    private final OutboxWriter outbox;
    private final EnvelopeCodec codec;
    private final Clock clock;

    public InventoryCommandHandler(ProcessedMessageRepository processedMessages, StockService stockService,
                                   OutboxWriter outbox, EnvelopeCodec codec, Clock clock) {
        this.processedMessages = processedMessages;
        this.stockService = stockService;
        this.outbox = outbox;
        this.codec = codec;
        this.clock = clock;
    }

    /**
     * Handles one command in ONE transaction: the duplicate check, the stock change and the reply (an outbox row)
     * are committed together, or not at all. A copy delivered again later finds its messageId and is skipped.
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
            case MessageTypes.RESERVE_STOCK -> reserveStock(envelope);
            default -> log.warn("Ignoring unknown command type '{}' (messageId={})", envelope.type(), envelope.messageId());
        }
    }

    private void reserveStock(MessageEnvelope envelope) {
        ReserveStock command = codec.payload(envelope, ReserveStock.class);
        ReservationResult result = stockService.reserve(envelope.orderId(), command.productId(), command.quantity());
        if (result.reserved()) {
            outbox.add(Topics.SAGA_REPLIES, envelope.orderId(), MessageTypes.STOCK_RESERVED,
                    new StockReserved(command.productId(), command.quantity()));
        } else {
            outbox.add(Topics.SAGA_REPLIES, envelope.orderId(), MessageTypes.STOCK_RESERVATION_FAILED,
                    new StockReservationFailed(command.productId(), command.quantity(), result.reason()));
        }
    }
}
