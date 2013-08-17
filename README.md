rabbitmq-autoreconnect-java
===========================

Helper to automatically handle reconnections automatically in a pool of rabbitmq servers.

To use it, simply instantiate RammitMQAutoConnection instead of using a ConnectionFactory with newConnection.
Everything will be automatic.

Maven configuration:

<repository>
    <id>Clever Cloud</id>
    <name>Clever Cloud's repository</name>
    <url>http://maven.clever-cloud.com</url>
    <layout>default</layout>
</repository>

<dependency>
    <groupId>com.clevercloud</groupId>
    <artifactId>rabbitmq-autoreconnect</artifactId>
    <version>1.3</version>
</dependency>

