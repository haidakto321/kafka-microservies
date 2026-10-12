package com.kafkademo.contracts;

/** Values of {@link MessageEnvelope#type()}. Constants so they can be used in switch statements. */
public final class MessageTypes {

    public static final String RESERVE_STOCK = "ReserveStock";
    public static final String STOCK_RESERVED = "StockReserved";
    public static final String STOCK_RESERVATION_FAILED = "StockReservationFailed";
    public static final String RELEASE_STOCK = "ReleaseStock";
    public static final String CHARGE_PAYMENT = "ChargePayment";
    public static final String PAYMENT_SUCCEEDED = "PaymentSucceeded";
    public static final String PAYMENT_FAILED = "PaymentFailed";
    public static final String ORDER_CONFIRMED = "OrderConfirmed";
    public static final String ORDER_CANCELLED = "OrderCancelled";

    private MessageTypes() {
    }
}
