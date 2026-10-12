package com.kafkademo.payment.payment;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PaymentTest {

    private static final Instant NOW = Instant.parse("2026-10-12T10:00:00Z");

    @Test
    void anAmountUnderTheLimitSucceeds() {
        Payment payment = Payment.charge(UUID.randomUUID(), new BigDecimal("50.00"), NOW);

        assertThat(payment.succeeded()).isTrue();
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
        assertThat(payment.getReason()).isNull();
    }

    @Test
    void exactlyTheLimitSucceeds() {
        assertThat(Payment.charge(UUID.randomUUID(), new BigDecimal("1000.00"), NOW).succeeded()).isTrue();
    }

    @Test
    void overTheLimitFailsWithAReason() {
        Payment payment = Payment.charge(UUID.randomUUID(), new BigDecimal("1000.01"), NOW);

        assertThat(payment.succeeded()).isFalse();
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(payment.getReason()).isEqualTo(Payment.OVER_LIMIT);
    }
}
