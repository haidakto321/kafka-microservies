package com.kafkademo.contracts.payload;

/** Reply: the stock was reserved. */
public record StockReserved(long productId, int quantity) {
}
