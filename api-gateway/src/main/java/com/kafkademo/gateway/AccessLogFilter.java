package com.kafkademo.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Spec 7.1: one log line per request - method, path, status and duration. A GlobalFilter runs for every route.
 * Reactive: chain.filter(...) returns at once; doFinally runs when the response is complete.
 */
@Component
public class AccessLogFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(AccessLogFilter.class);

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        long start = System.nanoTime();
        ServerHttpRequest request = exchange.getRequest(); // the original request, before StripPrefix
        return chain.filter(exchange).doFinally(signal -> {
            HttpStatusCode status = exchange.getResponse().getStatusCode();
            log.info("{} {} -> {} in {} ms", request.getMethod(), request.getURI().getRawPath(),
                    status == null ? "-" : status.value(), (System.nanoTime() - start) / 1_000_000);
        });
    }

    /** First filter in, last out - so the duration covers everything the gateway does. */
    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
