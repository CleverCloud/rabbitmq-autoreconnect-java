package com.clevercloud.rabbitmq;

import com.clevercloud.annotations.NonEmpty;
import com.rabbitmq.client.*;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

public class RabbitMQAutoConnection implements Connection, Watchable {

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

   private boolean verbose;

   private List<ShutdownListener> shutdownListeners;

   private Random random;

   public RabbitMQAutoConnection(@Nonnull @NonEmpty List<String> hosts, int port, String login, String password, long interval, long tries) {
      this.hosts = hosts;
      this.port = port;
      this.login = login;
      this.password = password;

      this.interval = interval;
      this.tries = tries;

      this.verbose = false;

      this.shutdownListeners = new ArrayList<>();

      this.random = new Random();

      new WatcherThread(this, this.interval).start();
   }

   public RabbitMQAutoConnection(@Nonnull @NonEmpty List<String> hosts, String login, String password, long interval, long tries) {
      this(hosts, ConnectionFactory.USE_DEFAULT_PORT, login, password, interval, tries);
   }

   public RabbitMQAutoConnection(@Nonnull @NonEmpty List<String> hosts, int port, String login, String password) {
      this(hosts, port, login, password, DEFAULT_INTERVAL, DEFAULT_TRIES);
   }

   public RabbitMQAutoConnection(@Nonnull @NonEmpty List<String> hosts, String login, String password) {
      this(hosts, login, password, DEFAULT_INTERVAL, DEFAULT_TRIES);
   }

   public void setVerbose(boolean verbose) {
      this.verbose = verbose;
   }

   public boolean isVerbose() {
      return this.verbose;
   }

   public long getTries() {
      return tries;
   }

   public long getInterval() {
      return interval;
   }

   private boolean isConnected() {
      return this.connection != null && this.connection.isOpen();
   }

   @Override
   public void watch() {
      this.checkConnection();
   }

   private synchronized void checkConnection() {
      if (this.isConnected())
         return;

      ConnectionFactory factory = new ConnectionFactory();
      factory.setPort(this.port);
      factory.setUsername(this.login);
      factory.setPassword(this.password);

      for (int tries = 0; (this.tries == NO_TRIES_LIMIT || tries < this.tries) && !this.isConnected(); ++tries) {
         if (this.verbose)
            Logger.getLogger(RabbitMQAutoConnection.class.getName()).info("Attempting to " + ((this.connection != null) ? "re" : "") + "connect to the rabbitmq server.");

         factory.setHost(this.hosts.get(this.random.nextInt(this.hosts.size())));

         try {
            this.connection = factory.newConnection(); // TODO: call newConnection with null, Address[] ?
         } catch (IOException ignored) {
         }

         try {
            Thread.sleep(this.interval);
         } catch (InterruptedException ignored) {
         }
      }

      if (this.isConnected()) {
         if (this.verbose)
            Logger.getLogger(RabbitMQAutoConnection.class.getName()).info("Connected to the rabbitmq server.");
      } else
         throw new NoRabbitMQConnectionException();
   }

   public Connection getConnection() {
      this.checkConnection();
      return this.connection;
   }

   /**
    * Retrieve the host.
    *
    * @return the hostname of the peer we're connected to.
    */
   @Override
   public InetAddress getAddress() {
      return this.getConnection().getAddress();
   }

   /**
    * Retrieve the port number.
    *
    * @return the port number of the peer we're connected to.
    */
   @Override
   public int getPort() {
      return this.getConnection().getPort();
   }

   /**
    * Get the negotiated maximum channel number. Usable channel
    * numbers range from 1 to this number, inclusive.
    *
    * @return the maximum channel number permitted for this connection.
    */
   @Override
   public int getChannelMax() {
      return this.getConnection().getChannelMax();
   }

   /**
    * Get the negotiated maximum frame size.
    *
    * @return the maximum frame size, in octets; zero if unlimited
    */
   @Override
   public int getFrameMax() {
      return this.getConnection().getFrameMax();
   }

   /**
    * Get the negotiated heartbeat interval.
    *
    * @return the heartbeat interval, in seconds; zero if none
    */
   @Override
   public int getHeartbeat() {
      return this.getConnection().getHeartbeat();
   }

   /**
    * Get a copy of the map of client properties sent to the server
    *
    * @return a copy of the map of client properties
    */
   @Override
   public Map<String, Object> getClientProperties() {
      return this.getConnection().getClientProperties();
   }

   /**
    * Retrieve the server properties.
    *
    * @return a map of the server properties. This typically includes the product name and version of the server.
    */
   @Override
   public Map<String, Object> getServerProperties() {
      return this.getConnection().getServerProperties();
   }

   public Channel createRawChannel() throws IOException {
      return this.getConnection().createChannel();
   }

   public Channel createRawChannel(int channelNumber) throws IOException {
      return this.getConnection().createChannel(channelNumber);
   }

   /**
    * Create a new channel, using an internally allocated channel number.
    *
    * @return a new channel descriptor, or null if none is available
    * @throws IOException if an I/O problem is encountered
    */
   @Override
   public Channel createChannel() throws IOException {
      return new RabbitMQAutoChannel(this);
   }

   /**
    * Create a new channel, using the specified channel number if possible.
    *
    * @param channelNumber the channel number to allocate
    * @return a new channel descriptor, or null if this channel number is already in use
    * @throws IOException if an I/O problem is encountered
    */
   @Override
   public Channel createChannel(int channelNumber) throws IOException {
      return new RabbitMQAutoChannel(this, channelNumber);
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
   @Override
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
   @Override
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
   @Override
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
   @Override
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
   @Override
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
   @Override
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
   @Override
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
   @Override
   public void abort(int closeCode, String closeMessage, int timeout) {
      this.getConnection().abort(closeCode, closeMessage, timeout);
   }

   /**
    * Add shutdown listener.
    * If the component is already closed, handler is fired immediately
    *
    * @param listener {@link com.rabbitmq.client.ShutdownListener} to the component
    */
   @Override
   public void addShutdownListener(ShutdownListener listener) {
      this.getConnection().addShutdownListener(listener);
      this.shutdownListeners.add(listener);
   }

   /**
    * Remove shutdown listener for the component.
    *
    * @param listener {@link com.rabbitmq.client.ShutdownListener} to be removed
    */
   @Override
   public void removeShutdownListener(ShutdownListener listener) {
      this.shutdownListeners.remove(listener);
      this.connection.removeShutdownListener(listener);
   }

   /**
    * Get the shutdown reason object
    *
    * @return ShutdownSignalException if component is closed, null otherwise
    */
   @Override
   public ShutdownSignalException getCloseReason() {
      return this.getConnection().getCloseReason();
   }

   /**
    * Protected API - notify the listeners attached to the component
    *
    * @see com.rabbitmq.client.ShutdownListener
    */
   @Override
   public void notifyListeners() {
      this.getConnection().notifyListeners();
   }

   /**
    * Determine whether the component is currently open.
    * Will return false if we are currently closing.
    * Checking this method should be only for information,
    * because of the race conditions - state can change after the call.
    * Instead just execute and try to catch ShutdownSignalException
    * and IOException
    *
    * @return true when component is open, false otherwise
    */
   @Override
   public boolean isOpen() {
      return this.getConnection().isOpen();
   }
}