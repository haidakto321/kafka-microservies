package com.kafkademo.contracts.payload;

/** Command: reserve this many items of this product for the order in the envelope. */
public record ReserveStock(long productId, int quantity) {
}
