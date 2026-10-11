package com.kafkademo.order.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jayway.jsonpath.JsonPath;
import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.ReserveStock;
import com.kafkademo.order.IntegrationTestContainers;
import com.kafkademo.order.order.OrderRepository;
import com.kafkademo.order.product.ProductClient;
import com.kafkademo.order.product.ProductInfo;
import com.kafkademo.order.product.ProductNotFoundException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@SpringBootTest
@AutoConfigureMockMvc
@Import(IntegrationTestContainers.class)
class OrderControllerTest {

    @Autowired
    MockMvc mvc;
    @Autowired
    OrderRepository orders;
    @Autowired
    EnvelopeCodec codec;
    @Autowired
    ConsumerFactory<String, String> consumerFactory;
    @Autowired
    JdbcTemplate jdbc;

    @MockitoBean
    ProductClient productClient;

    @Test
    void placingAnOrderSavesItPendingAndSendsReserveStock() throws Exception {
        when(productClient.getProduct(1L)).thenReturn(new ProductInfo(1L, "Keyboard", new BigDecimal("50.00"), 100));

        UUID orderId = placeOrder("{\"productId\":1,\"quantity\":2}");

        assertThat(orders.findById(orderId)).isPresent();
        MessageEnvelope command = awaitRecord(Topics.INVENTORY_COMMANDS, orderId);
        assertThat(command.type()).isEqualTo(MessageTypes.RESERVE_STOCK);
        assertThat(codec.payload(command, ReserveStock.class)).isEqualTo(new ReserveStock(1L, 2));
    }

    @Test
    void getReturnsThePlacedOrder() throws Exception {
        when(productClient.getProduct(1L)).thenReturn(new ProductInfo(1L, "Keyboard", new BigDecimal("50.00"), 100));
        UUID orderId = placeOrder("{\"productId\":1,\"quantity\":3}");

        mvc.perform(get("/orders/{id}", orderId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.total").value(150.0));
    }

    @Test
    void anUnknownOrderIs404() throws Exception {
        UUID unknown = UUID.randomUUID();

        // Checking the message matters: Spring also answers 404 when no endpoint exists at all.
        mvc.perform(get("/orders/{id}", unknown))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.detail").value("Order " + unknown + " not found"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"productId\":1,\"quantity\":0}",
            "{\"productId\":1,\"quantity\":-3}",
            "{\"quantity\":2}",
            "{\"productId\":0,\"quantity\":1}"
    })
    void invalidRequestsAre400AndSaveNothing(String body) throws Exception {
        long before = orders.count();

        mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest());

        assertThat(orders.count()).isEqualTo(before);
        verifyNoInteractions(productClient);
    }

    @Test
    void anUnknownProductIs400AndSavesNothing() throws Exception {
        when(productClient.getProduct(99L)).thenThrow(new ProductNotFoundException(99L));
        long ordersBefore = orders.count();
        long messagesBefore = jdbc.queryForObject("SELECT count(*) FROM outbox_event", Long.class);

        mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON).content("{\"productId\":99,\"quantity\":1}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value("Product 99 not found"));

        assertThat(orders.count()).isEqualTo(ordersBefore);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_event", Long.class)).isEqualTo(messagesBefore);
    }

    private UUID placeOrder(String body) throws Exception {
        MvcResult result = mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andReturn();
        String id = JsonPath.read(result.getResponse().getContentAsString(), "$.id");
        return UUID.fromString(id);
    }

    /** Reads a topic from the beginning until a record with this order's key shows up. */
    private MessageEnvelope awaitRecord(String topic, UUID orderId) {
        try (Consumer<String, String> consumer = consumerFactory.createConsumer("test-" + UUID.randomUUID(), null)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (orderId.toString().equals(record.key())) {
                        return codec.fromJson(record.value());
                    }
                }
            }
        }
        throw new AssertionError("No record on " + topic + " for order " + orderId);
    }
}
