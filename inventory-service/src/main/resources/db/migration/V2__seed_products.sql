-- Seed data from the spec (section 5).
INSERT INTO product (id, name, price, stock) VALUES
    (1, 'Keyboard',       50.00, 100),  -- happy path
    (2, 'Monitor',       300.00,   5),  -- run out of stock by ordering many
    (3, 'Laptop',       1500.00,  10),  -- always fails payment (M3)
    (4, 'Sold-out mug',   10.00,   0);  -- always out of stock
