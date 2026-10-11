package com.kafkademo.order.web;

import jakarta.validation.constraints.Positive;

/** Body of POST /orders. A missing field arrives as 0 and fails @Positive. */
public record PlaceOrderRequest(@Positive long productId, @Positive int quantity) {
}
