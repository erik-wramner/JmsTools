# JmsTools

This repository contains ready-to-use tools for benchmarking and testing JMS
providers and applications.

## Background

In 1996 I worked with IBM MQ Series using OS/2 and CICS. That was the first time I
worked with performance tuning for messaging. Since then I have been involved in
many projects with the same basic goal: make sure it works correctly (no lost
messages under any circumstances) and make it fast. It has never been my full-time
job, but as a consultant I have been there and done that a few times now.
The more memorable have been webMethods, AQ, HornetQ, ActiveMQ/AMQ and (again) AQ.
Every time I have ended up writing my own tools. Yes, every time. So, I'll do it again,
but this time I'll release the code and make it work for as many JMS providers
as possible. Or at least the ones I need.

## Main features

The current version includes a consumer and a producer for ActiveMQ, AQ JMS,
Apache Qpid with AMQP (covering Azure Service Bus and others), RabbitMQ
and IBM WebSphere MQ. They can run on any number of machines and as multiple
processes in order to scale if the configurable number of threads is not enough
(but usually it is - it is cheap to enqueue/dequeue without processing).
Some of the options:

* Unique message identities that make it possible to find lost messages, duplicate
  messages and ghost messages, i.e. messages that are delivered even though they
  have been rolled back.
* Checksums in order to check for corrupted messages.
* Prepared messages from a directory or random messages with a minimum and maximum size.
* Outliers (large messages with a certain probability).
* Delayed/scheduled messages (where supported).
* Text and byte messages, partial support for object messages and very partial support
  for map messages.
* Duration-based tests.
* Count-based tests.
* Configurable receive timeouts, polling delays, batch sizes, sleep times after each
  batch, constant throughput regulation and much more.
* Client-side producer flow control for AQ.
* XA transactions (two-phase commits) where supported.
* A log analyzer that helps make sense of the recorded test results.

Please check the [manual](Docs/JmsTools-Manual.adoc).

Feel free to submit pull requests, bug reports or to ask for other improvements.

## Missing jar files

Unfortunately Oracle keeps the Oracle JDBC drivers and the AQ JMS code private.
I can't publish the jar files and I can't download them from the Maven repository.
In order to use AQ you need to download the JDBC driver from Oracle and you need
to grab the correct version of aqapi.jar from you Oracle database. The AQ code
uses reflection, so the jar files are not used at build time. They need to be
included in the class path at runtime, though.
