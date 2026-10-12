package com.kafkademo.order.web;

import com.kafkademo.order.product.ProductNotFoundException;
import com.kafkademo.order.product.ProductServiceUnavailableException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {

    /** Ordering a product that doesn't exist is a bad request, not a server error. */
    @ExceptionHandler(ProductNotFoundException.class)
    ProblemDetail productNotFound(ProductNotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    /** inventory is down, slow or the breaker is open: tell the client to try again later. */
    @ExceptionHandler(ProductServiceUnavailableException.class)
    ProblemDetail productServiceUnavailable(ProductServiceUnavailableException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
    }
}
