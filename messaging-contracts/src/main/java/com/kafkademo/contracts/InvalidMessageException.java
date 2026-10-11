package com.kafkademo.contracts;

/** A Kafka message that can never be processed: not JSON, missing fields, or the wrong payload shape. */
public class InvalidMessageException extends RuntimeException {

    public InvalidMessageException(String message) {
        super(message);
    }

    public InvalidMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
