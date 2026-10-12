package com.kafkademo.contracts.payload;

import java.math.BigDecimal;

/** Event: the order is confirmed - stock reserved and paid. */
public record OrderConfirmed(long productId, int quantity, BigDecimal total) {
}
