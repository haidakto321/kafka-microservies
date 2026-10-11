package com.kafkademo.order.order;

import com.kafkademo.order.messaging.CommandPublisher;
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
    private final CommandPublisher commands;
    private final Clock clock;

    public OrderService(ProductClient productClient, OrderRepository orders, TransactionTemplate transactionTemplate,
                        CommandPublisher commands, Clock clock) {
        this.productClient = productClient;
        this.orders = orders;
        this.transactionTemplate = transactionTemplate;
        this.commands = commands;
        this.clock = clock;
    }

    /**
     * 1. Ask inventory for the price - OUTSIDE any DB transaction (never hold a DB connection during an HTTP call).
     * 2. Save the order as PENDING - an explicit transaction.
     * 3. Send ReserveStock.
     */
    public Order placeOrder(long productId, int quantity) {
        ProductInfo product = productClient.getProduct(productId);
        Order order = Order.place(UUID.randomUUID(), productId, quantity, product.price(), clock.instant());

        Order saved = transactionTemplate.execute(status -> orders.save(order));

        // M1: sent AFTER the commit, not together with it - the "dual write" problem.
        // If the app crashes right here, the order stays PENDING forever. M2 fixes this with the outbox.
        commands.reserveStock(saved.getId(), productId, quantity);
        log.info("Order {} placed: {} x product {}, total {}", saved.getId(), quantity, productId, saved.getTotal());
        return saved;
    }

    @Transactional(readOnly = true)
    public Optional<Order> find(UUID id) {
        return orders.findById(id);
    }
}
