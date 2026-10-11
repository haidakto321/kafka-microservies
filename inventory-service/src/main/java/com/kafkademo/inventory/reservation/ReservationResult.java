package com.kafkademo.inventory.reservation;

/** The answer to "reserve this stock": either reserved, or failed with a reason. */
public record ReservationResult(boolean reserved, String reason) {

    public static final String OUT_OF_STOCK = "OUT_OF_STOCK";
    public static final String UNKNOWN_PRODUCT = "UNKNOWN_PRODUCT";

    static ReservationResult success() {
        return new ReservationResult(true, null);
    }

    static ReservationResult failed(String reason) {
        return new ReservationResult(false, reason);
    }
}
