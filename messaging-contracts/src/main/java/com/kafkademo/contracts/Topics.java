package com.kafkademo.contracts;

/** Kafka topic names shared by all services. */
public final class Topics {

    /** order-service -> inventory-service: ReserveStock, ReleaseStock. */
    public static final String INVENTORY_COMMANDS = "inventory-commands";
    /** order-service -> payment-service: ChargePayment. */
    public static final String PAYMENT_COMMANDS = "payment-commands";
    /** inventory and payment -> order-service: the answers to the commands. */
    public static final String SAGA_REPLIES = "saga-replies";
    /** order-service -> anyone listening: OrderConfirmed, OrderCancelled. */
    public static final String ORDER_EVENTS = "order-events";

    /**
     * Dead-letter topics: a record that failed for good goes to "<topic>.DLT". Each service's error handler sets
     * this name explicitly - Spring Kafka 3.3's own default would be "<topic>-dlt".
     */
    public static final String DLT_SUFFIX = ".DLT";
    public static final String INVENTORY_COMMANDS_DLT = INVENTORY_COMMANDS + DLT_SUFFIX;
    public static final String SAGA_REPLIES_DLT = SAGA_REPLIES + DLT_SUFFIX;

    /** Every topic has 3 partitions, so up to 3 consumers per group can work in parallel. */
    public static final int PARTITIONS = 3;

    private Topics() {
    }
}
