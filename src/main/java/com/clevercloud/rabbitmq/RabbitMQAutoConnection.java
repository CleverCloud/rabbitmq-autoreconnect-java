package com.clevercloud.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;

public class RabbitMQAutoConnection {

   private Connection connection;

   private List<String> hosts;
   private String login;
   private String password;

   private Random random;

   public RabbitMQAutoConnection(List<String> hosts, String login, String password) throws IOException {   // TODO: port, timeout, retry
      this.hosts = hosts; // TODO: check for null
      this.login = login;
      this.password = password;

      this.random = new Random();

      this.checkConnection();
   }

   public void checkConnection() throws IOException {
      if (this.connection == null || !this.connection.isOpen()) { // TODO: loop with log and timeout
         ConnectionFactory factory = new ConnectionFactory();
         factory.setHost(this.hosts.get(this.random.nextInt(this.hosts.size())));
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