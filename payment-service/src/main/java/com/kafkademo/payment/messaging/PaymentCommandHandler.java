package com.kafkademo.payment.messaging;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.InvalidMessageException;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.ChargePayment;
import com.kafkademo.contracts.payload.PaymentFailed;
import com.kafkademo.contracts.payload.PaymentSucceeded;
import com.kafkademo.payment.outbox.OutboxWriter;
import com.kafkademo.payment.payment.Payment;
import com.kafkademo.payment.payment.PaymentRepository;
import java.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentCommandHandler {

    private static final Logger log = LoggerFactory.getLogger(PaymentCommandHandler.class);

    private final ProcessedMessageRepository processedMessages;
    private final PaymentRepository payments;
    private final OutboxWriter outbox;
    private final EnvelopeCodec codec;
    private final Clock clock;

    public PaymentCommandHandler(ProcessedMessageRepository processedMessages, PaymentRepository payments,
                                 OutboxWriter outbox, EnvelopeCodec codec, Clock clock) {
        this.processedMessages = processedMessages;
        this.payments = payments;
        this.outbox = outbox;
        this.codec = codec;
        this.clock = clock;
    }

    /**
     * Handles one command in ONE transaction: the duplicate check, the payment and the reply (an outbox row)
     * are committed together, or not at all.
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
            case MessageTypes.CHARGE_PAYMENT -> charge(envelope);
            default -> log.warn("Ignoring unknown command type '{}' (messageId={})", envelope.type(), envelope.messageId());
        }
    }

    private void charge(MessageEnvelope envelope) {
        ChargePayment command = codec.payload(envelope, ChargePayment.class);
        if (command.amount() == null) {
            // Invalid data can never succeed: not retryable, straight to the DLT (spec 6.4).
            throw new InvalidMessageException("ChargePayment " + envelope.messageId() + " has no amount");
        }
        Payment payment = payments.save(Payment.charge(envelope.orderId(), command.amount(), clock.instant()));
        if (payment.succeeded()) {
            outbox.add(Topics.SAGA_REPLIES, envelope.orderId(), MessageTypes.PAYMENT_SUCCEEDED,
                    new PaymentSucceeded(command.amount()));
        } else {
            outbox.add(Topics.SAGA_REPLIES, envelope.orderId(), MessageTypes.PAYMENT_FAILED,
                    new PaymentFailed(command.amount(), payment.getReason()));
        }
        log.info("Order {}: payment of {} {}", envelope.orderId(), command.amount(), payment.getStatus());
    }
}
