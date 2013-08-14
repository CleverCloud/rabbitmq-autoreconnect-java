package com.clevercloud.rabbitmq;

import com.clevercloud.annotations.NonEmpty;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

public class RabbitMQAutoConnection {

   /**
    * Retry until success
    */
   public static final long NO_TRIES_LIMIT = -1;

   /**
    * Default interval before two reconnection attempts
    */
   public static final long DEFAULT_INTERVAL = 500;

   /**
    * Default maximal number of tries to reconnect
    */
   public static final long DEFAULT_TRIES = NO_TRIES_LIMIT;

   private Connection connection;

   private List<String> hosts;
   private int port;
   private String login;
   private String password;

   private long interval;
   private long tries;

   private Random random;

   public RabbitMQAutoConnection(@Nonnull @NonEmpty List<String> hosts, int port, String login, String password, long interval, long tries) throws IOException {
      this.hosts = hosts;
      this.port = port;
      this.login = login;
      this.password = password;

      this.interval = interval;
      this.tries = tries;

      this.random = new Random();

      this.checkConnection();
   }

   public RabbitMQAutoConnection(@Nonnull @NonEmpty List<String> hosts, String login, String password, long interval, long tries) throws IOException {
      this(hosts, ConnectionFactory.USE_DEFAULT_PORT, login, password, interval, tries);
   }

   public RabbitMQAutoConnection(@Nonnull @NonEmpty List<String> hosts, String login, String password) throws IOException {
      this(hosts, login, password, DEFAULT_INTERVAL, DEFAULT_TRIES);
   }

   private boolean isConnected() {
      return this.connection != null && this.connection.isOpen();
   }

   public void checkConnection() throws IOException {
      for (int tries = 0; (this.tries == NO_TRIES_LIMIT || tries < this.tries) && !this.isConnected(); ++tries) {
         Logger.getLogger(RabbitMQAutoConnection.class.getName()).info("Attempting to " + ((this.connection != null) ? "re" : "") + "connect to the rabbitmq server.");

         ConnectionFactory factory = new ConnectionFactory();
         factory.setHost(this.hosts.get(this.random.nextInt(this.hosts.size())));
         factory.setPort(this.port);
         factory.setUsername(this.login);
         factory.setPassword(this.password);

         this.connection = factory.newConnection(); // TODO: call newConnextion with null, Address[] ?

         try {
            Thread.sleep(this.interval);
         } catch (InterruptedException ignored) {
         }
      }
      Logger.getLogger(RabbitMQAutoConnection.class.getName()).fine("Connected to the rabbitmq server.");
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