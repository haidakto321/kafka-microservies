package com.kafkademo.contracts.payload;

/** Command (compensation): put back the stock reserved for the order in the envelope. */
public record ReleaseStock(String reason) {
}
