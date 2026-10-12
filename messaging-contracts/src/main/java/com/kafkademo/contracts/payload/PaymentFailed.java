package com.kafkademo.contracts.payload;

import java.math.BigDecimal;

/** Reply: the payment failed. {@code reason} is AMOUNT_OVER_LIMIT. */
public record PaymentFailed(BigDecimal amount, String reason) {
}
