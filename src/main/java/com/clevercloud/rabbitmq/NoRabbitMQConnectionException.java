package com.clevercloud.rabbitmq;

/**
 * @author Marc-Antoine Perennou<Marc-Antoine@Perennou.com>
 */

public class NoRabbitMQConnectionException extends RuntimeException {

   public NoRabbitMQConnectionException() {
      super("Couldn't connect to rabbitmq server");
   }
}
