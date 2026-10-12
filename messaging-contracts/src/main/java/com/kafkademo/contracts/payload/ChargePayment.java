package com.kafkademo.contracts.payload;

import java.math.BigDecimal;

/** Command: charge the order's total. */
public record ChargePayment(BigDecimal amount) {
}
