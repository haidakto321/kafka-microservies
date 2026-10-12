package com.kafkademo.contracts.payload;

import java.math.BigDecimal;

/** Reply: the payment went through. */
public record PaymentSucceeded(BigDecimal amount) {
}
