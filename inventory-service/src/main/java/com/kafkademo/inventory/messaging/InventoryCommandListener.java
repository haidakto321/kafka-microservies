package com.kafkademo.inventory.messaging;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.ReserveStock;
import com.kafkademo.contracts.payload.StockReservationFailed;
import com.kafkademo.contracts.payload.StockReserved;
import com.kafkademo.inventory.reservation.ReservationResult;
import com.kafkademo.inventory.reservation.StockService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryCommandListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryCommandListener.class);

    private final EnvelopeCodec codec;
    private final StockService stockService;
    private final ReplyPublisher replies;

    public InventoryCommandListener(EnvelopeCodec codec, StockService stockService, ReplyPublisher replies) {
        this.codec = codec;
        this.stockService = stockService;
        this.replies = replies;
    }

    @KafkaListener(topics = Topics.INVENTORY_COMMANDS)
    public void onCommand(ConsumerRecord<String, String> record) {
        // partition + offset in the log make experiments E5 and E7 visible.
        log.info("Command received: partition={} offset={} key={}", record.partition(), record.offset(), record.key());
        MessageEnvelope envelope = codec.fromJson(record.value());
        switch (envelope.type()) {
            case MessageTypes.RESERVE_STOCK -> handleReserveStock(envelope);
            default -> log.warn("Ignoring unknown command type '{}' (messageId={})", envelope.type(), envelope.messageId());
        }
    }

    private void handleReserveStock(MessageEnvelope envelope) {
        ReserveStock command = codec.payload(envelope, ReserveStock.class);
        ReservationResult result = stockService.reserve(envelope.orderId(), command.productId(), command.quantity());
        // M1: the reply is sent AFTER the database commit, not together with it.
        // If the app crashes in between, the stock is reserved but no reply is ever sent. M2 fixes this (outbox).
        if (result.reserved()) {
            replies.send(envelope.orderId(), MessageTypes.STOCK_RESERVED,
                    new StockReserved(command.productId(), command.quantity()));
        } else {
            replies.send(envelope.orderId(), MessageTypes.STOCK_RESERVATION_FAILED,
                    new StockReservationFailed(command.productId(), command.quantity(), result.reason()));
        }
    }
}
