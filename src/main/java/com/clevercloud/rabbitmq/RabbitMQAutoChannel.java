package com.clevercloud.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

/**
 * @author Marc-Antoine Perennou<Marc-Antoine@Perennou.com>
 */

public class RabbitMQAutoChannel implements Channel, Watchable {

   private RabbitMQAutoConnection connection;
   private Integer channelNumber;

   private Channel channel;

   private List<ReturnListener> returnListeners;
   private List<FlowListener> flowListeners;
   private List<ConfirmListener> confirmListeners;
   private List<ShutdownListener> shutdownListeners;

   private Consumer defaultConsumer;
   private Map<String, BasicConsumer> consumers;

   private WatcherThread watcher;

   public RabbitMQAutoChannel(RabbitMQAutoConnection connection) {
      this.connection = connection;

      this.returnListeners = new ArrayList<ReturnListener>();
      this.flowListeners = new ArrayList<FlowListener>();
      this.confirmListeners = new ArrayList<ConfirmListener>();
      this.shutdownListeners = new ArrayList<ShutdownListener>();

      this.consumers = new HashMap<String, BasicConsumer>();

      this.watcher = new WatcherThread(this, this.connection.getInterval());
      this.watcher.start();
   }

   public RabbitMQAutoChannel(RabbitMQAutoConnection connection, Integer channelNumber) {
      this(connection);
      this.channelNumber = channelNumber;
   }

   private boolean isConnected() {
      return this.channel != null && this.channel.isOpen();
   }

   @Override
   public void watch() {
      this.checkChannel();
   }

   private synchronized void checkChannel() {
      if (this.isConnected())
         return;

      for (int tries = 0; (this.connection.getTries() == RabbitMQAutoConnection.NO_TRIES_LIMIT || tries < this.connection.getTries()) && !this.isConnected(); ++tries) {
         if (this.connection.isVerbose())
            Logger.getLogger(RabbitMQAutoConnection.class.getName()).info("Attempting to " + ((this.channel != null) ? "re" : "") + "create channel to the rabbitmq server.");

         try {
            this.channel = (this.channelNumber == null) ? this.connection.createRawChannel() : this.connection.createRawChannel(this.channelNumber);
            for (ReturnListener listener : this.returnListeners) {
               this.channel.addReturnListener(listener);
            }
            for (FlowListener listener : this.flowListeners) {
               this.channel.addFlowListener(listener);
            }
            for (ConfirmListener listener : this.confirmListeners) {
               this.channel.addConfirmListener(listener);
            }
            for (ShutdownListener listener : this.shutdownListeners) {
               this.channel.addShutdownListener(listener);
            }
            this.channel.setDefaultConsumer(this.defaultConsumer);
            for (Map.Entry<String, BasicConsumer> entry : this.consumers.entrySet()) {
               BasicConsumer c = entry.getValue();
               this.channel.basicConsume(entry.getKey(), c.autoack, c.consumerTag, c.noLocal, c.exclusive, c.arguments, c.consumer);
            }
         } catch (IOException ignored) {
         }

         try {
            Thread.sleep(this.connection.getInterval());
         } catch (InterruptedException ignored) {
         }
      }
      if (this.isConnected()) {
         if (this.connection.isVerbose())
            Logger.getLogger(RabbitMQAutoConnection.class.getName()).info("Created channel to the rabbitmq server.");
      } else
         throw new NoRabbitMQConnectionException();
   }

   public Channel getChannel() {
      this.checkChannel();
      return this.channel;
   }

   /**
    * Retrieve this channel's channel number.
    *
    * @return the channel number
    */
   @Override
   public int getChannelNumber() {
      return this.getChannel().getChannelNumber();
   }

   /**
    * Retrieve the connection which carries this channel.
    *
    * @return the underlying {@link com.rabbitmq.client.Connection}
    */
   @Override
   public Connection getConnection() {
      return this.getChannel().getConnection();
   }

   /**
    * Close this channel with the {@link com.rabbitmq.client.AMQP#REPLY_SUCCESS} close code
    * and message 'OK'.
    *
    * @throws java.io.IOException if an error is encountered
    */
   @Override
   public void close() throws IOException {
      this.watcher.cancel();
      this.getChannel().close();
   }

   /**
    * Close this channel.
    *
    * @param closeCode    the close code (See under "Reply Codes" in the AMQP specification)
    * @param closeMessage a message indicating the reason for closing the connection
    * @throws java.io.IOException if an error is encountered
    */
   @Override
   public void close(int closeCode, String closeMessage) throws IOException {
      this.watcher.cancel();
      this.getChannel().close(closeCode, closeMessage);
   }

   /**
    * Set flow on the channel
    *
    * @param active if true, the server is asked to start sending. If false, the server is asked to stop sending.
    * @throws java.io.IOException
    */
   @Override
   public AMQP.Channel.FlowOk flow(boolean active) throws IOException {
      return this.getChannel().flow(active);
   }

   /**
    * Return the current Channel.Flow settings.
    */
   @Override
   public AMQP.Channel.FlowOk getFlow() {
      return this.getChannel().getFlow();
   }

   /**
    * Abort this channel with the {@link com.rabbitmq.client.AMQP#REPLY_SUCCESS} close code
    * and message 'OK'.
    * <p/>
    * Forces the channel to close and waits for the close operation to complete.
    * Any encountered exceptions in the close operation are silently discarded.
    */
   @Override
   public void abort() throws IOException {
      this.watcher.cancel();
      this.getChannel().abort();
   }

   /**
    * Abort this channel.
    * <p/>
    * Forces the channel to close and waits for the close operation to complete.
    * Any encountered exceptions in the close operation are silently discarded.
    */
   @Override
   public void abort(int closeCode, String closeMessage) throws IOException {
      this.watcher.cancel();
      this.getChannel().abort(closeCode, closeMessage);
   }

   /**
    * Add a {@link com.rabbitmq.client.ReturnListener}.
    *
    * @param listener the listener to add
    */
   @Override
   public void addReturnListener(ReturnListener listener) {
      this.getChannel().addReturnListener(listener);
      this.returnListeners.add(listener);
   }

   /**
    * Remove a {@link com.rabbitmq.client.ReturnListener}.
    *
    * @param listener the listener to remove
    * @return <code><b>true</b></code> if the listener was found and removed,
    *         <code><b>false</b></code> otherwise
    */
   @Override
   public boolean removeReturnListener(ReturnListener listener) {
      this.returnListeners.remove(listener);
      return this.getChannel().removeReturnListener(listener);
   }

   /**
    * Remove all {@link com.rabbitmq.client.ReturnListener}s.
    */
   @Override
   public void clearReturnListeners() {
      this.returnListeners.clear();
      this.getChannel().clearReturnListeners();
   }

   /**
    * Add a {@link com.rabbitmq.client.FlowListener}.
    *
    * @param listener the listener to add
    */
   @Override
   public void addFlowListener(FlowListener listener) {
      this.getChannel().addFlowListener(listener);
      this.flowListeners.add(listener);
   }

   /**
    * Remove a {@link com.rabbitmq.client.FlowListener}.
    *
    * @param listener the listener to remove
    * @return <code><b>true</b></code> if the listener was found and removed,
    *         <code><b>false</b></code> otherwise
    */
   @Override
   public boolean removeFlowListener(FlowListener listener) {
      this.flowListeners.remove(listener);
      return this.getChannel().removeFlowListener(listener);
   }

   /**
    * Remove all {@link com.rabbitmq.client.FlowListener}s.
    */
   @Override
   public void clearFlowListeners() {
      this.flowListeners.clear();
      this.getChannel().clearFlowListeners();
   }

   /**
    * Add a {@link com.rabbitmq.client.ConfirmListener}.
    *
    * @param listener the listener to add
    */
   @Override
   public void addConfirmListener(ConfirmListener listener) {
      this.getChannel().addConfirmListener(listener);
      this.confirmListeners.add(listener);
   }

   /**
    * Remove a {@link com.rabbitmq.client.ConfirmListener}.
    *
    * @param listener the listener to remove
    * @return <code><b>true</b></code> if the listener was found and removed,
    *         <code><b>false</b></code> otherwise
    */
   @Override
   public boolean removeConfirmListener(ConfirmListener listener) {
      this.confirmListeners.remove(listener);
      return this.getChannel().removeConfirmListener(listener);
   }

   /**
    * Remove all {@link com.rabbitmq.client.ConfirmListener}s.
    */
   @Override
   public void clearConfirmListeners() {
      this.confirmListeners.clear();
      this.getChannel().clearConfirmListeners();
   }

   /**
    * Get the current default consumer. @see setDefaultConsumer for rationale.
    *
    * @return an interface to the current default consumer.
    */
   @Override
   public Consumer getDefaultConsumer() {
      return this.getChannel().getDefaultConsumer();
   }

   /**
    * Set the current default consumer.
    * <p/>
    * Under certain circumstances it is possible for a channel to receive a
    * message delivery which does not match any consumer which is currently
    * set up via basicConsume(). This will occur after the following sequence
    * of events:
    * <p/>
    * ctag = basicConsume(queue, consumer); // i.e. with explicit acks
    * // some deliveries take place but are not acked
    * basicCancel(ctag);
    * basicRecover(false);
    * <p/>
    * Since requeue is specified to be false in the basicRecover, the spec
    * states that the message must be redelivered to "the original recipient"
    * - i.e. the same channel / consumer-tag. But the consumer is no longer
    * active.
    * <p/>
    * In these circumstances, you can register a default consumer to handle
    * such deliveries. If no default consumer is registered an
    * IllegalStateException will be thrown when such a delivery arrives.
    * <p/>
    * Most people will not need to use this.
    *
    * @param consumer the consumer to use, or null indicating "don't use one".
    */
   @Override
   public void setDefaultConsumer(Consumer consumer) {
      this.defaultConsumer = consumer;
      this.getChannel().setDefaultConsumer(consumer);
   }

   /**
    * Request specific "quality of service" settings.
    * <p/>
    * These settings impose limits on the amount of data the server
    * will deliver to consumers before requiring acknowledgements.
    * Thus they provide a means of consumer-initiated flow control.
    *
    * @param prefetchSize  maximum amount of content (measured in
    *                      octets) that the server will deliver, 0 if unlimited
    * @param prefetchCount maximum number of messages that the server
    *                      will deliver, 0 if unlimited
    * @param global        true if the settings should be applied to the
    *                      entire connection rather than just the current channel
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Basic.Qos
    */
   @Override
   public void basicQos(int prefetchSize, int prefetchCount, boolean global) throws IOException {
      try {
         this.getChannel().basicQos(prefetchSize, prefetchCount, global);
      } catch (IOException connectionReset) {
         this.getChannel().basicQos(prefetchSize, prefetchCount, global);
      }
   }

   /**
    * Request a specific prefetchCount "quality of service" settings
    * for this channel.
    *
    * @param prefetchCount maximum number of messages that the server
    *                      will deliver, 0 if unlimited
    * @throws java.io.IOException if an error is encountered
    * @see #basicQos(int, int, boolean)
    */
   @Override
   public void basicQos(int prefetchCount) throws IOException {
      try {
         this.getChannel().basicQos(prefetchCount);
      } catch (IOException connectionReset) {
         this.getChannel().basicQos(prefetchCount);
      }
   }

   /**
    * Publish a message
    *
    * @param exchange   the exchange to publish the message to
    * @param routingKey the routing key
    * @param props      other properties for the message - routing headers etc
    * @param body       the message body
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Basic.Publish
    */
   @Override
   public void basicPublish(String exchange, String routingKey, AMQP.BasicProperties props, byte[] body) throws IOException {
      try {
         this.getChannel().basicPublish(exchange, routingKey, props, body);
      } catch (IOException connectionReset) {
         this.getChannel().basicPublish(exchange, routingKey, props, body);
      }
   }

   /**
    * Publish a message
    *
    * @param exchange   the exchange to publish the message to
    * @param routingKey the routing key
    * @param mandatory  true if the 'mandatory' flag is to be set
    * @param props      other properties for the message - routing headers etc
    * @param body       the message body
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Basic.Publish
    */
   @Override
   public void basicPublish(String exchange, String routingKey, boolean mandatory, AMQP.BasicProperties props, byte[] body) throws IOException {
      try {
         this.getChannel().basicPublish(exchange, routingKey, mandatory, props, body);
      } catch (IOException connectionReset) {
         this.getChannel().basicPublish(exchange, routingKey, mandatory, props, body);
      }
   }

   /**
    * Publish a message
    *
    * @param exchange   the exchange to publish the message to
    * @param routingKey the routing key
    * @param mandatory  true if the 'mandatory' flag is to be set
    * @param immediate  true if the 'immediate' flag is to be
    *                   set. Note that the RabbitMQ server does not support this flag.
    * @param props      other properties for the message - routing headers etc
    * @param body       the message body
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Basic.Publish
    */
   @Override
   public void basicPublish(String exchange, String routingKey, boolean mandatory, boolean immediate, AMQP.BasicProperties props, byte[] body) throws IOException {
      try {
         this.getChannel().basicPublish(exchange, routingKey, mandatory, immediate, props, body);
      } catch (IOException connectionReset) {
         this.getChannel().basicPublish(exchange, routingKey, mandatory, immediate, props, body);
      }
   }

   /**
    * Actively declare a non-autodelete, non-durable exchange with no extra arguments
    *
    * @param exchange the name of the exchange
    * @param type     the exchange type
    * @return a declaration-confirm method to indicate the exchange was successfully declared
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Exchange.Declare
    * @see com.rabbitmq.client.AMQP.Exchange.DeclareOk
    */
   @Override
   public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type) throws IOException {
      try {
         return this.getChannel().exchangeDeclare(exchange, type);
      } catch (IOException connectionReset) {
         return this.getChannel().exchangeDeclare(exchange, type);
      }
   }

   /**
    * Actively declare a non-autodelete exchange with no extra arguments
    *
    * @param exchange the name of the exchange
    * @param type     the exchange type
    * @param durable  true if we are declaring a durable exchange (the exchange will survive a server restart)
    * @return a declaration-confirm method to indicate the exchange was successfully declared
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Exchange.Declare
    * @see com.rabbitmq.client.AMQP.Exchange.DeclareOk
    */
   @Override
   public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable) throws IOException {
      try {
         return this.getChannel().exchangeDeclare(exchange, type, durable);
      } catch (IOException connectionReset) {
         return this.getChannel().exchangeDeclare(exchange, type, durable);
      }
   }

   /**
    * Declare an exchange.
    *
    * @param exchange   the name of the exchange
    * @param type       the exchange type
    * @param durable    true if we are declaring a durable exchange (the exchange will survive a server restart)
    * @param autoDelete true if the server should delete the exchange when it is no longer in use
    * @param arguments  other properties (construction arguments) for the exchange
    * @return a declaration-confirm method to indicate the exchange was successfully declared
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Exchange.Declare
    * @see com.rabbitmq.client.AMQP.Exchange.DeclareOk
    */
   @Override
   public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete, Map<String, Object> arguments) throws IOException {
      try {
         return this.getChannel().exchangeDeclare(exchange, type, durable, autoDelete, arguments);
      } catch (IOException connectionReset) {
         return this.getChannel().exchangeDeclare(exchange, type, durable, autoDelete, arguments);
      }
   }

   /**
    * Declare an exchange, via an interface that allows the complete set of
    * arguments.
    *
    * @param exchange   the name of the exchange
    * @param type       the exchange type
    * @param durable    true if we are declaring a durable exchange (the exchange will survive a server restart)
    * @param autoDelete true if the server should delete the exchange when it is no longer in use
    * @param internal   true if the exchange is internal, i.e. can't be directly
    *                   published to by a client.
    * @param arguments  other properties (construction arguments) for the exchange
    * @return a declaration-confirm method to indicate the exchange was successfully declared
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Exchange.Declare
    * @see com.rabbitmq.client.AMQP.Exchange.DeclareOk
    */
   @Override
   public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) throws IOException {
      try {
         return this.getChannel().exchangeDeclare(exchange, type, durable, autoDelete, internal, arguments);
      } catch (IOException connectionReset) {
         return this.getChannel().exchangeDeclare(exchange, type, durable, autoDelete, internal, arguments);
      }
   }

   /**
    * Declare an exchange passively; that is, check if the named exchange exists.
    *
    * @param name check the existence of an exchange named this
    * @throws java.io.IOException the server will raise a 404 channel exception if the named exchange does not exist.
    */
   @Override
   public AMQP.Exchange.DeclareOk exchangeDeclarePassive(String name) throws IOException {
      try {
         return this.getChannel().exchangeDeclarePassive(name);
      } catch (IOException connectionReset) {
         return this.getChannel().exchangeDeclarePassive(name);
      }
   }

   /**
    * Delete an exchange
    *
    * @param exchange the name of the exchange
    * @param ifUnused true to indicate that the exchange is only to be deleted if it is unused
    * @return a deletion-confirm method to indicate the exchange was successfully deleted
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Exchange.Delete
    * @see com.rabbitmq.client.AMQP.Exchange.DeleteOk
    */
   @Override
   public AMQP.Exchange.DeleteOk exchangeDelete(String exchange, boolean ifUnused) throws IOException {
      try {
         return this.getChannel().exchangeDelete(exchange, ifUnused);
      } catch (IOException connectionReset) {
         return this.getChannel().exchangeDelete(exchange, ifUnused);
      }
   }

   /**
    * Delete an exchange, without regard for whether it is in use or not
    *
    * @param exchange the name of the exchange
    * @return a deletion-confirm method to indicate the exchange was successfully deleted
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Exchange.Delete
    * @see com.rabbitmq.client.AMQP.Exchange.DeleteOk
    */
   @Override
   public AMQP.Exchange.DeleteOk exchangeDelete(String exchange) throws IOException {
      try {
         return this.getChannel().exchangeDelete(exchange);
      } catch (IOException connectionReset) {
         return this.getChannel().exchangeDelete(exchange);
      }
   }

   /**
    * Bind an exchange to an exchange, with no extra arguments.
    *
    * @param destination the name of the exchange to which messages flow across the binding
    * @param source      the name of the exchange from which messages flow across the binding
    * @param routingKey  the routine key to use for the binding
    * @return a binding-confirm method if the binding was successfully created
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Exchange.Bind
    * @see com.rabbitmq.client.AMQP.Exchange.BindOk
    */
   @Override
   public AMQP.Exchange.BindOk exchangeBind(String destination, String source, String routingKey) throws IOException {
      try {
         return this.getChannel().exchangeBind(destination, source, routingKey);
      } catch (IOException connectionReset) {
         return this.getChannel().exchangeBind(destination, source, routingKey);
      }
   }

   /**
    * Bind an exchange to an exchange.
    *
    * @param destination the name of the exchange to which messages flow across the binding
    * @param source      the name of the exchange from which messages flow across the binding
    * @param routingKey  the routine key to use for the binding
    * @param arguments   other properties (binding parameters)
    * @return a binding-confirm method if the binding was successfully created
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Exchange.Bind
    * @see com.rabbitmq.client.AMQP.Exchange.BindOk
    */
   @Override
   public AMQP.Exchange.BindOk exchangeBind(String destination, String source, String routingKey, Map<String, Object> arguments) throws IOException {
      try {
         return this.getChannel().exchangeBind(destination, source, routingKey, arguments);
      } catch (IOException connectionReset) {
         return this.getChannel().exchangeBind(destination, source, routingKey, arguments);
      }
   }

   /**
    * Unbind an exchange from an exchange, with no extra arguments.
    *
    * @param destination the name of the exchange to which messages flow across the binding
    * @param source      the name of the exchange from which messages flow across the binding
    * @param routingKey  the routine key to use for the binding
    * @return a binding-confirm method if the binding was successfully created
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Exchange.Bind
    * @see com.rabbitmq.client.AMQP.Exchange.BindOk
    */
   @Override
   public AMQP.Exchange.UnbindOk exchangeUnbind(String destination, String source, String routingKey) throws IOException {
      try {
         return this.getChannel().exchangeUnbind(destination, source, routingKey);
      } catch (IOException connectionReset) {
         return this.getChannel().exchangeUnbind(destination, source, routingKey);
      }
   }

   /**
    * Unbind an exchange from an exchange.
    *
    * @param destination the name of the exchange to which messages flow across the binding
    * @param source      the name of the exchange from which messages flow across the binding
    * @param routingKey  the routine key to use for the binding
    * @param arguments   other properties (binding parameters)
    * @return a binding-confirm method if the binding was successfully created
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Exchange.Bind
    * @see com.rabbitmq.client.AMQP.Exchange.BindOk
    */
   @Override
   public AMQP.Exchange.UnbindOk exchangeUnbind(String destination, String source, String routingKey, Map<String, Object> arguments) throws IOException {
      try {
         return this.getChannel().exchangeUnbind(destination, source, routingKey, arguments);
      } catch (IOException connectionReset) {
         return this.getChannel().exchangeUnbind(destination, source, routingKey, arguments);
      }
   }

   /**
    * Actively declare a server-named exclusive, autodelete, non-durable queue.
    * The name of the new queue is held in the "queue" field of the {@link com.rabbitmq.client.AMQP.Queue.DeclareOk} result.
    *
    * @return a declaration-confirm method to indicate the queue was successfully declared
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Queue.Declare
    * @see com.rabbitmq.client.AMQP.Queue.DeclareOk
    */
   @Override
   public AMQP.Queue.DeclareOk queueDeclare() throws IOException {
      try {
         return this.getChannel().queueDeclare();
      } catch (IOException connectionReset) {
         return this.getChannel().queueDeclare();
      }
   }

   /**
    * Declare a queue
    *
    * @param queue      the name of the queue
    * @param durable    true if we are declaring a durable queue (the queue will survive a server restart)
    * @param exclusive  true if we are declaring an exclusive queue (restricted to this connection)
    * @param autoDelete true if we are declaring an autodelete queue (server will delete it when no longer in use)
    * @param arguments  other properties (construction arguments) for the queue
    * @return a declaration-confirm method to indicate the queue was successfully declared
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Queue.Declare
    * @see com.rabbitmq.client.AMQP.Queue.DeclareOk
    */
   @Override
   public AMQP.Queue.DeclareOk queueDeclare(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) throws IOException {
      try {
         return this.getChannel().queueDeclare(queue, durable, exclusive, autoDelete, arguments);
      } catch (IOException connectionReset) {
         return this.getChannel().queueDeclare(queue, durable, exclusive, autoDelete, arguments);
      }
   }

   /**
    * Declare a queue passively; i.e., check if it exists.  In AMQP
    * 0-9-1, all arguments aside from nowait are ignored; and sending
    * nowait makes this method a no-op, so we default it to false.
    *
    * @param queue the name of the queue
    * @return a declaration-confirm method to indicate the queue exists
    * @throws java.io.IOException if an error is encountered,
    *                             including if the queue does not exist and if the queue is
    *                             exclusively owned by another connection.
    * @see com.rabbitmq.client.AMQP.Queue.Declare
    * @see com.rabbitmq.client.AMQP.Queue.DeclareOk
    */
   @Override
   public AMQP.Queue.DeclareOk queueDeclarePassive(String queue) throws IOException {
      try {
         return this.getChannel().queueDeclarePassive(queue);
      } catch (IOException connectionReset) {
         return this.getChannel().queueDeclarePassive(queue);
      }
   }

   /**
    * Delete a queue, without regard for whether it is in use or has messages on it
    *
    * @param queue the name of the queue
    * @return a deletion-confirm method to indicate the queue was successfully deleted
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Queue.Delete
    * @see com.rabbitmq.client.AMQP.Queue.DeleteOk
    */
   @Override
   public AMQP.Queue.DeleteOk queueDelete(String queue) throws IOException {
      try {
         return this.getChannel().queueDelete(queue);
      } catch (IOException connectionReset) {
         return this.getChannel().queueDelete(queue);
      }
   }

   /**
    * Delete a queue
    *
    * @param queue    the name of the queue
    * @param ifUnused true if the queue should be deleted only if not in use
    * @param ifEmpty  true if the queue should be deleted only if empty
    * @return a deletion-confirm method to indicate the queue was successfully deleted
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Queue.Delete
    * @see com.rabbitmq.client.AMQP.Queue.DeleteOk
    */
   @Override
   public AMQP.Queue.DeleteOk queueDelete(String queue, boolean ifUnused, boolean ifEmpty) throws IOException {
      try {
         return this.getChannel().queueDelete(queue, ifUnused, ifEmpty);
      } catch (IOException connectionReset) {
         return this.getChannel().queueDelete(queue, ifUnused, ifEmpty);
      }
   }

   /**
    * Bind a queue to an exchange, with no extra arguments.
    *
    * @param queue      the name of the queue
    * @param exchange   the name of the exchange
    * @param routingKey the routine key to use for the binding
    * @return a binding-confirm method if the binding was successfully created
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Queue.Bind
    * @see com.rabbitmq.client.AMQP.Queue.BindOk
    */
   @Override
   public AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey) throws IOException {
      try {
         return this.getChannel().queueBind(queue, exchange, routingKey);
      } catch (IOException connectionReset) {
         return this.getChannel().queueBind(queue, exchange, routingKey);
      }
   }

   /**
    * Bind a queue to an exchange.
    *
    * @param queue      the name of the queue
    * @param exchange   the name of the exchange
    * @param routingKey the routine key to use for the binding
    * @param arguments  other properties (binding parameters)
    * @return a binding-confirm method if the binding was successfully created
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Queue.Bind
    * @see com.rabbitmq.client.AMQP.Queue.BindOk
    */
   @Override
   public AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey, Map<String, Object> arguments) throws IOException {
      try {
         return this.getChannel().queueBind(queue, exchange, routingKey, arguments);
      } catch (IOException connectionReset) {
         return this.getChannel().queueBind(queue, exchange, routingKey, arguments);
      }
   }

   /**
    * Unbinds a queue from an exchange, with no extra arguments.
    *
    * @param queue      the name of the queue
    * @param exchange   the name of the exchange
    * @param routingKey the routine key to use for the binding
    * @return an unbinding-confirm method if the binding was successfully deleted
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Queue.Unbind
    * @see com.rabbitmq.client.AMQP.Queue.UnbindOk
    */
   @Override
   public AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey) throws IOException {
      try {
         return this.getChannel().queueUnbind(queue, exchange, routingKey);
      } catch (IOException connectionReset) {
         return this.getChannel().queueUnbind(queue, exchange, routingKey);
      }
   }

   /**
    * Unbind a queue from an exchange.
    *
    * @param queue      the name of the queue
    * @param exchange   the name of the exchange
    * @param routingKey the routine key to use for the binding
    * @param arguments  other properties (binding parameters)
    * @return an unbinding-confirm method if the binding was successfully deleted
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Queue.Unbind
    * @see com.rabbitmq.client.AMQP.Queue.UnbindOk
    */
   @Override
   public AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey, Map<String, Object> arguments) throws IOException {
      try {
         return this.getChannel().queueUnbind(queue, exchange, routingKey, arguments);
      } catch (IOException connectionReset) {
         return this.getChannel().queueUnbind(queue, exchange, routingKey, arguments);
      }
   }

   /**
    * Purges the contents of the given queue.
    *
    * @param queue the name of the queue
    * @return a purge-confirm method if the purge was executed succesfully
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Queue.Purge
    * @see com.rabbitmq.client.AMQP.Queue.PurgeOk
    */
   @Override
   public AMQP.Queue.PurgeOk queuePurge(String queue) throws IOException {
      try {
         return this.getChannel().queuePurge(queue);
      } catch (IOException connectionReset) {
         return this.getChannel().queuePurge(queue);
      }
   }

   /**
    * Retrieve a message from a queue using {@link com.rabbitmq.client.AMQP.Basic.Get}
    *
    * @param queue   the name of the queue
    * @param autoAck true if the server should consider messages
    *                acknowledged once delivered; false if the server should expect
    *                explicit acknowledgements
    * @return a {@link com.rabbitmq.client.GetResponse} containing the retrieved message data
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Basic.Get
    * @see com.rabbitmq.client.AMQP.Basic.GetOk
    * @see com.rabbitmq.client.AMQP.Basic.GetEmpty
    */
   @Override
   public GetResponse basicGet(String queue, boolean autoAck) throws IOException {
      try {
         return this.getChannel().basicGet(queue, autoAck);
      } catch (IOException connectionReset) {
         return this.getChannel().basicGet(queue, autoAck);
      }
   }

   /**
    * Acknowledge one or several received
    * messages. Supply the deliveryTag from the {@link com.rabbitmq.client.AMQP.Basic.GetOk}
    * or {@link com.rabbitmq.client.AMQP.Basic.Deliver} method
    * containing the received message being acknowledged.
    *
    * @param deliveryTag the tag from the received {@link com.rabbitmq.client.AMQP.Basic.GetOk} or {@link com.rabbitmq.client.AMQP.Basic.Deliver}
    * @param multiple    true to acknowledge all messages up to and
    *                    including the supplied delivery tag; false to acknowledge just
    *                    the supplied delivery tag.
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Basic.Ack
    */
   @Override
   public void basicAck(long deliveryTag, boolean multiple) throws IOException {
      try {
         this.getChannel().basicAck(deliveryTag, multiple);
      } catch (IOException connectionReset) {
         this.getChannel().basicAck(deliveryTag, multiple);
      }
   }

   /**
    * Reject one or several received messages.
    * <p/>
    * Supply the <code>deliveryTag</code> from the {@link com.rabbitmq.client.AMQP.Basic.GetOk}
    * or {@link com.rabbitmq.client.AMQP.Basic.GetOk} method containing the message to be rejected.
    *
    * @param deliveryTag the tag from the received {@link com.rabbitmq.client.AMQP.Basic.GetOk} or {@link com.rabbitmq.client.AMQP.Basic.Deliver}
    * @param multiple    true to reject all messages up to and including
    *                    the supplied delivery tag; false to reject just the supplied
    *                    delivery tag.
    * @param requeue     true if the rejected message(s) should be requeued rather
    *                    than discarded/dead-lettered
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Basic.Nack
    */
   @Override
   public void basicNack(long deliveryTag, boolean multiple, boolean requeue) throws IOException {
      try {
         this.getChannel().basicNack(deliveryTag, multiple, requeue);
      } catch (IOException connectionReset) {
         this.getChannel().basicNack(deliveryTag, multiple, requeue);
      }
   }

   /**
    * Reject a message. Supply the deliveryTag from the {@link com.rabbitmq.client.AMQP.Basic.GetOk}
    * or {@link com.rabbitmq.client.AMQP.Basic.Deliver} method
    * containing the received message being rejected.
    *
    * @param deliveryTag the tag from the received {@link com.rabbitmq.client.AMQP.Basic.GetOk} or {@link com.rabbitmq.client.AMQP.Basic.Deliver}
    * @param requeue     true if the rejected message should be requeued rather than discarded/dead-lettered
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Basic.Reject
    */
   @Override
   public void basicReject(long deliveryTag, boolean requeue) throws IOException {
      try {
         this.getChannel().basicReject(deliveryTag, requeue);
      } catch (IOException connectionReset) {
         this.getChannel().basicReject(deliveryTag, requeue);
      }
   }

   /**
    * Start a non-nolocal, non-exclusive consumer, with
    * explicit acknowledgement and a server-generated consumerTag.
    *
    * @param queue    the name of the queue
    * @param callback an interface to the consumer object
    * @return the consumerTag generated by the server
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Basic.Consume
    * @see com.rabbitmq.client.AMQP.Basic.ConsumeOk
    * @see #basicAck
    * @see #basicConsume(String, boolean, String, boolean, boolean, java.util.Map, com.rabbitmq.client.Consumer)
    */
   @Override
   public String basicConsume(String queue, Consumer callback) throws IOException {
      String ret;
      try {
         ret = this.getChannel().basicConsume(queue, callback);
      } catch (IOException connectionReset) {
         ret = this.getChannel().basicConsume(queue, callback);
      }
      this.consumers.put(queue, new BasicConsumer(callback));
      return ret;
   }

   /**
    * Start a non-nolocal, non-exclusive consumer, with
    * a server-generated consumerTag.
    *
    * @param queue    the name of the queue
    * @param autoAck  true if the server should consider messages
    *                 acknowledged once delivered; false if the server should expect
    *                 explicit acknowledgements
    * @param callback an interface to the consumer object
    * @return the consumerTag generated by the server
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Basic.Consume
    * @see com.rabbitmq.client.AMQP.Basic.ConsumeOk
    * @see #basicConsume(String, boolean, String, boolean, boolean, java.util.Map, com.rabbitmq.client.Consumer)
    */
   @Override
   public String basicConsume(String queue, boolean autoAck, Consumer callback) throws IOException {
      String ret;
      try {
         ret = this.getChannel().basicConsume(queue, autoAck, callback);
      } catch (IOException connectionReset) {
         ret = this.getChannel().basicConsume(queue, autoAck, callback);
      }
      this.consumers.put(queue, new BasicConsumer(callback, autoAck));
      return ret;
   }

   /**
    * Start a non-nolocal, non-exclusive consumer.
    *
    * @param queue       the name of the queue
    * @param autoAck     true if the server should consider messages
    *                    acknowledged once delivered; false if the server should expect
    *                    explicit acknowledgements
    * @param consumerTag a client-generated consumer tag to establish context
    * @param callback    an interface to the consumer object
    * @return the consumerTag associated with the new consumer
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Basic.Consume
    * @see com.rabbitmq.client.AMQP.Basic.ConsumeOk
    * @see #basicConsume(String, boolean, String, boolean, boolean, java.util.Map, com.rabbitmq.client.Consumer)
    */
   @Override
   public String basicConsume(String queue, boolean autoAck, String consumerTag, Consumer callback) throws IOException {
      String ret;
      try {
         ret = this.getChannel().basicConsume(queue, autoAck, consumerTag, callback);
      } catch (IOException connectionReset) {
         ret = this.getChannel().basicConsume(queue, autoAck, consumerTag, callback);
      }
      this.consumers.put(queue, new BasicConsumer(callback, autoAck, consumerTag));
      return ret;
   }

   /**
    * Start a consumer. Calls the consumer's {@link com.rabbitmq.client.Consumer#handleConsumeOk}
    * method.
    *
    * @param queue       the name of the queue
    * @param autoAck     true if the server should consider messages
    *                    acknowledged once delivered; false if the server should expect
    *                    explicit acknowledgements
    * @param consumerTag a client-generated consumer tag to establish context
    * @param noLocal     true if the server should not deliver to this consumer
    *                    messages published on this channel's connection
    * @param exclusive   true if this is an exclusive consumer
    * @param callback    an interface to the consumer object
    * @param arguments   a set of arguments for the consume
    * @return the consumerTag associated with the new consumer
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Basic.Consume
    * @see com.rabbitmq.client.AMQP.Basic.ConsumeOk
    */
   @Override
   public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, Consumer callback) throws IOException {
      String ret;
      try {
         ret = this.getChannel().basicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments, callback);
      } catch (IOException connectionReset) {
         ret = this.getChannel().basicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments, callback);
      }
      this.consumers.put(queue, new BasicConsumer(callback, autoAck, consumerTag, noLocal, exclusive, arguments));
      return ret;
   }

   /**
    * Cancel a consumer. Calls the consumer's {@link com.rabbitmq.client.Consumer#handleCancelOk}
    * method.
    *
    * @param consumerTag a client- or server-generated consumer tag to establish context
    * @throws java.io.IOException if an error is encountered, or if the consumerTag is unknown
    * @see com.rabbitmq.client.AMQP.Basic.Cancel
    * @see com.rabbitmq.client.AMQP.Basic.CancelOk
    */
   @Override
   public void basicCancel(String consumerTag) throws IOException {
      try {
         this.getChannel().basicCancel(consumerTag);
      } catch (IOException connectionReset) {
         this.getChannel().basicCancel(consumerTag);
      }
   }

   /**
    * Ask the broker to resend unacknowledged messages.  In 0-8
    * basic.recover is asynchronous; in 0-9-1 it is synchronous, and
    * the new, deprecated method basic.recover_async is asynchronous.
    * <p/>
    * Equivalent to calling <code>basicRecover(true)</code>, messages
    * will be requeued and possibly delivered to a different consumer.
    *
    * @see #basicRecover(boolean)
    */
   @Override
   public AMQP.Basic.RecoverOk basicRecover() throws IOException {
      try {
         return this.getChannel().basicRecover();
      } catch (IOException connectionReset) {
         return this.getChannel().basicRecover();
      }
   }

   /**
    * Ask the broker to resend unacknowledged messages.  In 0-8
    * basic.recover is asynchronous; in 0-9-1 it is synchronous, and
    * the new, deprecated method basic.recover_async is asynchronous.
    *
    * @param requeue If true, messages will be requeued and possibly
    *                delivered to a different consumer. If false, messages will be
    *                redelivered to the same consumer.
    */
   @Override
   public AMQP.Basic.RecoverOk basicRecover(boolean requeue) throws IOException {
      try {
         return this.getChannel().basicRecover(requeue);
      } catch (IOException connectionReset) {
         return this.getChannel().basicRecover(requeue);
      }
   }

   /**
    * Ask the broker to resend unacknowledged messages.  In 0-8
    * basic.recover is asynchronous; in 0-9-1 it is synchronous, and
    * the new, deprecated method basic.recover_async is asynchronous
    * and deprecated.
    *
    * @param requeue If true, messages will be requeued and possibly
    *                delivered to a different consumer. If false, messages will be
    *                redelivered to the same consumer.
    */
   @Override
   public void basicRecoverAsync(boolean requeue) throws IOException {
      try {
         this.getChannel().basicRecoverAsync(requeue);
      } catch (IOException connectionReset) {
         this.getChannel().basicRecoverAsync(requeue);
      }
   }

   /**
    * Enables TX mode on this channel.
    *
    * @return a transaction-selection method to indicate the transaction was successfully initiated
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Tx.Select
    * @see com.rabbitmq.client.AMQP.Tx.SelectOk
    */
   @Override
   public AMQP.Tx.SelectOk txSelect() throws IOException {
      try {
         return this.getChannel().txSelect();
      } catch (IOException connectionReset) {
         return this.getChannel().txSelect();
      }
   }

   /**
    * Commits a TX transaction on this channel.
    *
    * @return a transaction-commit method to indicate the transaction was successfully committed
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Tx.Commit
    * @see com.rabbitmq.client.AMQP.Tx.CommitOk
    */
   @Override
   public AMQP.Tx.CommitOk txCommit() throws IOException {
      try {
         return this.getChannel().txCommit();
      } catch (IOException connectionReset) {
         return this.getChannel().txCommit();
      }
   }

   /**
    * Rolls back a TX transaction on this channel.
    *
    * @return a transaction-rollback method to indicate the transaction was successfully rolled back
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Tx.Rollback
    * @see com.rabbitmq.client.AMQP.Tx.RollbackOk
    */
   @Override
   public AMQP.Tx.RollbackOk txRollback() throws IOException {
      try {
         return this.getChannel().txRollback();
      } catch (IOException connectionReset) {
         return this.getChannel().txRollback();
      }
   }

   /**
    * Enables publisher acknowledgements on this channel.
    *
    * @throws java.io.IOException if an error is encountered
    * @see com.rabbitmq.client.AMQP.Confirm.Select
    */
   @Override
   public AMQP.Confirm.SelectOk confirmSelect() throws IOException {
      try {
         return this.getChannel().confirmSelect();
      } catch (IOException connectionReset) {
         return this.getChannel().confirmSelect();
      }
   }

   /**
    * When in confirm mode, returns the sequence number of the next
    * message to be published.
    *
    * @return the sequence number of the next message to be published
    */
   @Override
   public long getNextPublishSeqNo() {
      return this.getChannel().getNextPublishSeqNo();
   }

   /**
    * Wait until all messages published since the last call have been
    * either ack'd or nack'd by the broker.  Note, when called on a
    * non-Confirm channel, waitForConfirms throws an IllegalStateException.
    *
    * @return whether all the messages were ack'd (and none were nack'd)
    * @throws IllegalStateException
    */
   @Override
   public boolean waitForConfirms() throws InterruptedException {
      return this.getChannel().waitForConfirms();
   }

   /**
    * Wait until all messages published since the last call have been
    * either ack'd or nack'd by the broker; or until timeout elapses.
    * If the timeout expires a TimeoutException is thrown.  When
    * called on a non-Confirm channel, waitForConfirms throws an
    * IllegalStateException.
    *
    * @return whether all the messages were ack'd (and none were nack'd)
    * @throws IllegalStateException
    */
   @Override
   public boolean waitForConfirms(long timeout) throws InterruptedException, TimeoutException {
      return this.getChannel().waitForConfirms(timeout);
   }

   /**
    * Wait until all messages published since the last call have
    * been either ack'd or nack'd by the broker.  If any of the
    * messages were nack'd, waitForConfirmsOrDie will throw an
    * IOException.  When called on a non-Confirm channel, it will
    * throw an IllegalStateException.
    *
    * @throws IllegalStateException
    */
   @Override
   public void waitForConfirmsOrDie() throws IOException, InterruptedException {
      try {
         this.getChannel().waitForConfirmsOrDie();
      } catch (IOException connectionReset) {
         this.getChannel().waitForConfirmsOrDie();
      }
   }

   /**
    * Wait until all messages published since the last call have
    * been either ack'd or nack'd by the broker; or until timeout elapses.
    * If the timeout expires a TimeoutException is thrown.  If any of the
    * messages were nack'd, waitForConfirmsOrDie will throw an
    * IOException.  When called on a non-Confirm channel, it will
    * throw an IllegalStateException.
    *
    * @throws IllegalStateException
    */
   @Override
   public void waitForConfirmsOrDie(long timeout) throws IOException, InterruptedException, TimeoutException {
      try {
         this.getChannel().waitForConfirmsOrDie(timeout);
      } catch (IOException connectionReset) {
         this.getChannel().waitForConfirmsOrDie(timeout);
      }
   }

   /**
    * Asynchronously send a method over this channel.
    *
    * @param method method to transmit over this channel.
    * @throws java.io.IOException Problem transmitting method.
    */
   @Override
   public void asyncRpc(Method method) throws IOException {
      try {
         this.getChannel().asyncRpc(method);
      } catch (IOException connectionReset) {
         this.getChannel().asyncRpc(method);
      }
   }

   /**
    * Synchronously send a method over this channel.
    *
    * @param method method to transmit over this channel.
    * @return command response to method. Caller should cast as appropriate.
    * @throws java.io.IOException Problem transmitting method.
    */
   @Override
   public Command rpc(Method method) throws IOException {
      try {
         return this.getChannel().rpc(method);
      } catch (IOException connectionReset) {
         return this.getChannel().rpc(method);
      }
   }

   /**
    * Add shutdown listener.
    * If the component is already closed, handler is fired immediately
    *
    * @param listener {@link com.rabbitmq.client.ShutdownListener} to the component
    */
   @Override
   public void addShutdownListener(ShutdownListener listener) {
      this.getChannel().addShutdownListener(listener);
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
      this.getChannel().removeShutdownListener(listener);
   }

   /**
    * Get the shutdown reason object
    *
    * @return ShutdownSignalException if component is closed, null otherwise
    */
   @Override
   public ShutdownSignalException getCloseReason() {
      return this.getChannel().getCloseReason();
   }

   /**
    * Protected API - notify the listeners attached to the component
    *
    * @see com.rabbitmq.client.ShutdownListener
    */
   @Override
   public void notifyListeners() {
      this.getChannel().notifyListeners();
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
      return this.getChannel().isOpen();
   }

   private class BasicConsumer {

      private Consumer consumer;
      private boolean autoack;
      private String consumerTag;
      private boolean noLocal;
      private boolean exclusive;
      private Map<String, Object> arguments;

      public BasicConsumer(Consumer consumer, boolean autoack, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments) {
         this.consumer = consumer;
         this.autoack = autoack;
         this.consumerTag = consumerTag;
         this.noLocal = noLocal;
         this.exclusive = exclusive;
         this.arguments = arguments;
      }

      public BasicConsumer(Consumer consumer, boolean autoack, String consumerTag) {
         this(consumer, autoack, consumerTag, false, false, null);
      }

      public BasicConsumer(Consumer consumer, boolean autoack) {
         this(consumer, autoack, "");
      }

      public BasicConsumer(Consumer consumer) {
         this(consumer, true);
      }
   }
}
