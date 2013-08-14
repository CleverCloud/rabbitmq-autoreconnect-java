package com.clevercloud.rabbitmq;

import com.clevercloud.annotations.NonEmpty;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Random;

public class RabbitMQAutoConnection {

   /**
    * Taken from ConnectionFactory:
    * 'Use the default port' port
    */
   public static final int USE_DEFAULT_PORT = -1;

   private Connection connection;

   private List<String> hosts;
   private int port;
   private String login;
   private String password;

   private Random random;

   public RabbitMQAutoConnection(@Nonnull @NonEmpty List<String> hosts, int port, String login, String password) throws IOException {   // TODO: timeout, retry
      this.hosts = hosts;
      this.port = port;
      this.login = login;
      this.password = password;

      this.random = new Random();

      this.checkConnection();
   }

   public RabbitMQAutoConnection(@Nonnull @NonEmpty List<String> hosts, String login, String password) throws IOException {
      this(hosts, USE_DEFAULT_PORT, login, password);
   }

   public void checkConnection() throws IOException {
      if (this.connection == null || !this.connection.isOpen()) { // TODO: loop with log and timeout
         ConnectionFactory factory = new ConnectionFactory();
         factory.setHost(this.hosts.get(this.random.nextInt(this.hosts.size())));
         factory.setPort(this.port);
         factory.setUsername(this.login);
         factory.setPassword(this.password);

         this.connection = factory.newConnection();
      }
   }

   public Connection getConnection() throws IOException {
      this.checkConnection();
      return this.connection;
   }

   public Channel createChannel() throws IOException {
      return this.getConnection().createChannel();
   }

   public void close() throws IOException {
      if (this.connection != null && this.connection.isOpen()) {
         this.connection.close();
      }
   }
}