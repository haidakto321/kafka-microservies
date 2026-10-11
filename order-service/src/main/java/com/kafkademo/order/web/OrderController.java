package com.kafkademo.order.web;

import com.kafkademo.order.order.Order;
import com.kafkademo.order.order.OrderService;
import jakarta.validation.Valid;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    /** 202 Accepted: the order is saved, but the saga runs in the background. Check GET /orders/{id} later. */
    @PostMapping
    public ResponseEntity<OrderView> place(@Valid @RequestBody PlaceOrderRequest request) {
        Order order = orderService.placeOrder(request.productId(), request.quantity());
        return ResponseEntity.accepted().body(OrderView.from(order));
    }

    @GetMapping("/{id}")
    public OrderView get(@PathVariable UUID id) {
        return orderService.find(id)
                .map(OrderView::from)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Order " + id + " not found"));
    }
}
