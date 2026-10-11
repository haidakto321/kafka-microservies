package com.kafkademo.order.order;

import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.ReserveStock;
import com.kafkademo.order.outbox.OutboxWriter;
import com.kafkademo.order.product.ProductClient;
import com.kafkademo.order.product.ProductInfo;
import java.time.Clock;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final ProductClient productClient;
    private final OrderRepository orders;
    private final TransactionTemplate transactionTemplate;
    private final OutboxWriter outbox;
    private final Clock clock;

    public OrderService(ProductClient productClient, OrderRepository orders, TransactionTemplate transactionTemplate,
                        OutboxWriter outbox, Clock clock) {
        this.productClient = productClient;
        this.orders = orders;
        this.transactionTemplate = transactionTemplate;
        this.outbox = outbox;
        this.clock = clock;
    }

    /**
     * 1. Ask inventory for the price - OUTSIDE any DB transaction (never hold a DB connection during an HTTP call).
     * 2. In ONE transaction: save the order as PENDING and its ReserveStock message (an outbox row).
     *    Both are saved, or neither. The outbox relay sends the message afterwards, so Kafka can be down
     *    and the order is still accepted.
     */
    public Order placeOrder(long productId, int quantity) {
        ProductInfo product = productClient.getProduct(productId);
        Order order = Order.place(UUID.randomUUID(), productId, quantity, product.price(), clock.instant());

        Order saved = transactionTemplate.execute(status -> {
            Order created = orders.save(order);
            outbox.add(Topics.INVENTORY_COMMANDS, created.getId(), MessageTypes.RESERVE_STOCK,
                    new ReserveStock(productId, quantity));
            return created;
        });
        log.info("Order {} placed: {} x product {}, total {}", saved.getId(), quantity, productId, saved.getTotal());
        return saved;
    }

    @Transactional(readOnly = true)
    public Optional<Order> find(UUID id) {
        return orders.findById(id);
    }
}
