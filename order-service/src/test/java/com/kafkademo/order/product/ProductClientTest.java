package com.kafkademo.order.product;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

class ProductClientTest {

    @Test
    void readsAProduct() {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        server.expect(requestTo("http://inventory/products/1"))
                .andRespond(withSuccess("{\"id\":1,\"name\":\"Keyboard\",\"price\":50.00,\"stock\":100}",
                        MediaType.APPLICATION_JSON));
        ProductClient client = new ProductClient(builder, "http://inventory");

        assertThat(client.getProduct(1L)).isEqualTo(new ProductInfo(1L, "Keyboard", new BigDecimal("50.00"), 100));
        server.verify();
    }

    @Test
    void a404BecomesProductNotFound() {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        server.expect(requestTo("http://inventory/products/99")).andRespond(withStatus(HttpStatus.NOT_FOUND));
        ProductClient client = new ProductClient(builder, "http://inventory");

        assertThatThrownBy(() -> client.getProduct(99L))
                .isInstanceOf(ProductNotFoundException.class)
                .hasMessage("Product 99 not found");
    }
}
