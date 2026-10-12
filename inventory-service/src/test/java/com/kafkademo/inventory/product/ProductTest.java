package com.kafkademo.inventory.product;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class ProductTest {

    private Product productWithStock(int stock) {
        return new Product(1L, "Keyboard", new BigDecimal("50.00"), stock);
    }

    @Test
    void hasStockWhenEnoughOrExactlyEnough() {
        assertThat(productWithStock(5).hasStock(4)).isTrue();
        assertThat(productWithStock(5).hasStock(5)).isTrue();
    }

    @Test
    void hasNoStockWhenTooFew() {
        assertThat(productWithStock(5).hasStock(6)).isFalse();
        assertThat(productWithStock(0).hasStock(1)).isFalse();
    }

    @Test
    void zeroOrNegativeQuantityIsNeverAvailable() {
        assertThat(productWithStock(5).hasStock(0)).isFalse();
        assertThat(productWithStock(5).hasStock(-1)).isFalse();
    }

    @Test
    void decreaseStockTakesItemsAway() {
        Product product = productWithStock(5);

        product.decreaseStock(5);

        assertThat(product.getStock()).isZero();
    }

    @Test
    void decreaseStockRefusesToGoBelowZero() {
        Product product = productWithStock(2);

        assertThatThrownBy(() -> product.decreaseStock(3))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Not enough stock");
        assertThat(product.getStock()).isEqualTo(2);
    }

    @Test
    void increaseStockPutsItemsBack() {
        Product product = productWithStock(2);

        product.increaseStock(3);

        assertThat(product.getStock()).isEqualTo(5);
    }

    @Test
    void increaseStockRejectsZeroOrLess() {
        Product product = productWithStock(2);

        assertThatThrownBy(() -> product.increaseStock(0)).isInstanceOf(IllegalArgumentException.class);
        assertThat(product.getStock()).isEqualTo(2);
    }
}
