package com.clevercloud.rabbitmq;

import com.clevercloud.annotations.NonEmpty;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
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

   public RabbitMQAutoConnection(@Nonnull @NonEmpty List<String> hosts, int port, String login, String password, long interval, long tries) {
      this.hosts = hosts;
      this.port = port;
      this.login = login;
      this.password = password;

      this.interval = interval;
      this.tries = tries;

      this.random = new Random();
   }

   public RabbitMQAutoConnection(@Nonnull @NonEmpty List<String> hosts, String login, String password, long interval, long tries) {
      this(hosts, ConnectionFactory.USE_DEFAULT_PORT, login, password, interval, tries);
   }

   public RabbitMQAutoConnection(@Nonnull @NonEmpty List<String> hosts, String login, String password) {
      this(hosts, login, password, DEFAULT_INTERVAL, DEFAULT_TRIES);
   }

   private boolean isConnected() {
      return this.connection != null && this.connection.isOpen();
   }

   public void checkConnection() {
      if (this.isConnected())
         return;

      for (int tries = 0; (this.tries == NO_TRIES_LIMIT || tries < this.tries) && !this.isConnected(); ++tries) {
         Logger.getLogger(RabbitMQAutoConnection.class.getName()).info("Attempting to " + ((this.connection != null) ? "re" : "") + "connect to the rabbitmq server.");

         ConnectionFactory factory = new ConnectionFactory();
         factory.setHost(this.hosts.get(this.random.nextInt(this.hosts.size())));
         factory.setPort(this.port);
         factory.setUsername(this.login);
         factory.setPassword(this.password);

         try {
            this.connection = factory.newConnection(); // TODO: call newConnection with null, Address[] ?
         } catch (IOException ignored) {
         }

         try {
            Thread.sleep(this.interval);
         } catch (InterruptedException ignored) {
         }
      }
      if (this.isConnected())
         Logger.getLogger(RabbitMQAutoConnection.class.getName()).info("Connected to the rabbitmq server.");
      // TODO else exception
   }

   public Connection getConnection() {
      this.checkConnection();
      return this.connection;
   }

   /**
    * Wrapper around Connection methods
    * Comments taken from Connection
    */

   /**
    * Retrieve the host.
    *
    * @return the hostname of the peer we're connected to.
    */
   public InetAddress getAddress() {
      return this.getConnection().getAddress();
   }

   /**
    * Retrieve the port number.
    *
    * @return the port number of the peer we're connected to.
    */

   public int getPort() {
      return this.getConnection().getPort();
   }

   /**
    * Get the negotiated maximum channel number. Usable channel
    * numbers range from 1 to this number, inclusive.
    *
    * @return the maximum channel number permitted for this connection.
    */
   public int getChannelMax() {
      return this.getConnection().getChannelMax();
   }

   /**
    * Get the negotiated maximum frame size.
    *
    * @return the maximum frame size, in octets; zero if unlimited
    */
   public int getFrameMax() {
      return this.getConnection().getFrameMax();
   }

   /**
    * Get the negotiated heartbeat interval.
    *
    * @return the heartbeat interval, in seconds; zero if none
    */
   public int getHeartbeat() {
      return this.getConnection().getHeartbeat();
   }

   /**
    * Get a copy of the map of client properties sent to the server
    *
    * @return a copy of the map of client properties
    */
   public Map<String, Object> getClientProperties() {
      return this.getConnection().getClientProperties();
   }

   /**
    * Retrieve the server properties.
    *
    * @return a map of the server properties. This typically includes the product name and version of the server.
    */
   public Map<String, Object> getServerProperties() {
      return this.getConnection().getServerProperties();
   }

   /**
    * Create a new channel, using an internally allocated channel number.
    *
    * @return a new channel descriptor, or null if none is available
    * @throws IOException if an I/O problem is encountered
    */
   public Channel createChannel() throws IOException { // FIXME: wrapper
      return this.getConnection().createChannel();
   }

   /**
    * Create a new channel, using the specified channel number if possible.
    *
    * @param channelNumber the channel number to allocate
    * @return a new channel descriptor, or null if this channel number is already in use
    * @throws IOException if an I/O problem is encountered
    */
   public Channel createChannel(int channelNumber) throws IOException {  // FIXME: wrapper
      return this.getConnection().createChannel(channelNumber);
   }

   /**
    * Close this connection and all its channels
    * with the {@link com.rabbitmq.client.AMQP#REPLY_SUCCESS} close code
    * and message 'OK'.
    * <p/>
    * Waits for all the close operations to complete.
    *
    * @throws IOException if an I/O problem is encountered
    */
   public void close() throws IOException {
      this.getConnection().close();
   }

   /**
    * Close this connection and all its channels.
    * <p/>
    * Waits for all the close operations to complete.
    *
    * @param closeCode    the close code (See under "Reply Codes" in the AMQP specification)
    * @param closeMessage a message indicating the reason for closing the connection
    * @throws IOException if an I/O problem is encountered
    */
   public void close(int closeCode, String closeMessage) throws IOException {
      this.getConnection().close(closeCode, closeMessage);
   }

   /**
    * Close this connection and all its channels
    * with the {@link com.rabbitmq.client.AMQP#REPLY_SUCCESS} close code
    * and message 'OK'.
    * <p/>
    * This method behaves in a similar way as {@link #close()}, with the only difference
    * that it waits with a provided timeout for all the close operations to
    * complete. When timeout is reached the socket is forced to close.
    *
    * @param timeout timeout (in milliseconds) for completing all the close-related
    *                operations, use -1 for infinity
    * @throws IOException if an I/O problem is encountered
    */
   public void close(int timeout) throws IOException {
      this.getConnection().close(timeout);
   }

   /**
    * Close this connection and all its channels.
    * <p/>
    * Waits with the given timeout for all the close operations to complete.
    * When timeout is reached the socket is forced to close.
    *
    * @param closeCode    the close code (See under "Reply Codes" in the AMQP specification)
    * @param closeMessage a message indicating the reason for closing the connection
    * @param timeout      timeout (in milliseconds) for completing all the close-related
    *                     operations, use -1 for infinity
    * @throws IOException if an I/O problem is encountered
    */
   public void close(int closeCode, String closeMessage, int timeout) throws IOException {
      this.getConnection().close(closeCode, closeMessage, timeout);
   }

   /**
    * Abort this connection and all its channels
    * with the {@link com.rabbitmq.client.AMQP#REPLY_SUCCESS} close code
    * and message 'OK'.
    * <p/>
    * Forces the connection to close.
    * Any encountered exceptions in the close operations are silently discarded.
    */
   public void abort() {
      this.getConnection().abort();
   }

   /**
    * Abort this connection and all its channels.
    * <p/>
    * Forces the connection to close and waits for all the close operations to complete.
    * Any encountered exceptions in the close operations are silently discarded.
    *
    * @param closeCode    the close code (See under "Reply Codes" in the AMQP specification)
    * @param closeMessage a message indicating the reason for closing the connection
    */
   public void abort(int closeCode, String closeMessage) {
      this.getConnection().abort(closeCode, closeMessage);
   }

   /**
    * Abort this connection and all its channels
    * with the {@link com.rabbitmq.client.AMQP#REPLY_SUCCESS} close code
    * and message 'OK'.
    * <p/>
    * This method behaves in a similar way as {@link #abort()}, with the only difference
    * that it waits with a provided timeout for all the close operations to
    * complete. When timeout is reached the socket is forced to close.
    *
    * @param timeout timeout (in milliseconds) for completing all the close-related
    *                operations, use -1 for infinity
    */
   public void abort(int timeout) {
      this.getConnection().abort(timeout);
   }

   /**
    * Abort this connection and all its channels.
    * <p/>
    * Forces the connection to close and waits with the given timeout
    * for all the close operations to complete. When timeout is reached
    * the socket is forced to close.
    * Any encountered exceptions in the close operations are silently discarded.
    *
    * @param closeCode    the close code (See under "Reply Codes" in the AMQP specification)
    * @param closeMessage a message indicating the reason for closing the connection
    * @param timeout      timeout (in milliseconds) for completing all the close-related
    *                     operations, use -1 for infinity
    */
   public void abort(int closeCode, String closeMessage, int timeout) {
      this.getConnection().abort(closeCode, closeMessage, timeout);
   }
}