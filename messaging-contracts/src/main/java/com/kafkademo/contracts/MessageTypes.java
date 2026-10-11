package com.kafkademo.contracts;

/** Values of {@link MessageEnvelope#type()}. Constants so they can be used in switch statements. */
public final class MessageTypes {

    public static final String RESERVE_STOCK = "ReserveStock";
    public static final String STOCK_RESERVED = "StockReserved";
    public static final String STOCK_RESERVATION_FAILED = "StockReservationFailed";

    private MessageTypes() {
    }
}
