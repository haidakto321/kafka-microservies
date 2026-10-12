package com.kafkademo.contracts.payload;

/** Event: the order is cancelled. {@code reason} is OUT_OF_STOCK or PAYMENT_FAILED. */
public record OrderCancelled(String reason) {
}
