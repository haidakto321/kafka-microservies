package com.kafkademo.contracts.payload;

/** Reply: the stock could not be reserved. {@code reason} is OUT_OF_STOCK or UNKNOWN_PRODUCT. */
public record StockReservationFailed(long productId, int quantity, String reason) {
}
