/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package net.ssorj.quiver;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.Message;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;
import io.vertx.proton.ProtonServer;

public class QuiverArrowVertxProton {
    private static final String CLIENT = "client";
    private static final String SERVER = "server";
    private static final String ACTIVE = "active";
    private static final String RECEIVE = "receive";
    private static final String SEND = "send";

    private static final Accepted ACCEPTED = Accepted.getInstance();

    public static void main(final String[] args) {
        try {
            doMain(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void doMain(final String[] args) throws Exception {
        final String connectionMode = args[0];
        final String channelMode = args[1];
        final String operation = args[2];
        final String id = args[3];
        final String host = args[4];
        final int port = Integer.parseInt(args[5]);
        final String path = args[6];
        final int desiredDuration = Integer.parseInt(args[7]);
        final int desiredCount = Integer.parseInt(args[8]);
        final int bodySize = Integer.parseInt(args[9]);
        final int creditWindow = Integer.parseInt(args[10]);
        final int transactionSize = Integer.parseInt(args[11]);
        final String[] flags = args[12].split(",");

        final boolean durable = Arrays.asList(flags).contains("durable");

        if (transactionSize > 0) {
            throw new RuntimeException("This impl doesn't support transactions");
        }

        final boolean clientMode;
        if (CLIENT.equalsIgnoreCase(connectionMode)) {
            clientMode = true;
        } else if (SERVER.equalsIgnoreCase(connectionMode)) {
            clientMode = false;
        } else {
            throw new java.lang.IllegalStateException("Unknown mode: " + connectionMode);
        }

        final boolean activeMode = ACTIVE.equalsIgnoreCase(channelMode);
        if (clientMode && !activeMode) {
            throw new RuntimeException("The client mode currently supports active mode only");
        } else if (!clientMode && activeMode) {
            throw new RuntimeException("The server mode currently supports passive mode only");
        }

        final boolean sendMode;
        if (SEND.equalsIgnoreCase(operation)) {
            sendMode = true;
        } else if (RECEIVE.equalsIgnoreCase(operation)) {
            sendMode = false;
        } else {
            throw new java.lang.IllegalStateException("Unknown operation: " + operation);
        }

        final CountDownLatch completionLatch = new CountDownLatch(1);
        final Vertx vertx = Vertx.vertx(new VertxOptions().setPreferNativeTransport(true));

        if(clientMode) {
            handleClientMode(id, host, port, path, desiredDuration, desiredCount, bodySize, creditWindow, durable, sendMode, completionLatch, vertx);
        } else {
            handleServerMode(id, host, port, path, desiredDuration, desiredCount, bodySize, creditWindow, durable, sendMode, completionLatch, vertx);
        }

        // Await the operations completing, then shut down the Vertx instance.
        completionLatch.await();

        vertx.close();
    }

    private static void handleClientMode(final String id, final String host, final int port, final String path,
                                         final int desiredDuration, final int desiredCount, final int bodySize, final int creditWindow,
                                         final boolean durable, final boolean sendMode, final CountDownLatch completionLatch, final Vertx vertx) {
        ProtonClient client = ProtonClient.create(vertx);
        client.connect(host, port, (res) -> {
                if (res.succeeded()) {
                    final ProtonConnection connection = res.result();

                    connection.setContainer(id);
                    connection.closeHandler(x -> {
                        completionLatch.countDown();
                    });

                    if (desiredDuration > 0) {
                        vertx.setTimer(desiredDuration * 1000, timerId -> {
                            connection.close();
                        });
                    }

                    connection.open();

                    if (sendMode) {
                        ProtonSender sender = connection.createSender(path);
                        send(true, connection, sender, desiredCount, bodySize, durable);
                    } else {
                        ProtonReceiver receiver = connection.createReceiver(path);
                        receive(true, connection, receiver, desiredCount, creditWindow);
                    }
                } else {
                    res.cause().printStackTrace();
                    completionLatch.countDown();
                }
            });
    }


    private static void handleServerMode(final String id, final String host, final int port, final String path,
                                         final int desiredDuration, final int desiredCount, final int bodySize, final int creditWindow,
                                         final boolean durable, final boolean sendMode, final CountDownLatch completionLatch, final Vertx vertx) {
        ProtonServer server = ProtonServer.create(vertx);
        // Configure how new connections are handled
        server.connectHandler((connection) -> {
            if (desiredDuration > 0) {
                vertx.setTimer(desiredDuration * 1000, timerId -> {
                    // TODO: No way to unbind-only. Not nice to remotely-close or sever the client connection.
                    // Just awaiting the client closing connection currently (see below).
                });
            }

            connection.openHandler(res -> {
                connection.open();
              });

              connection.closeHandler(c -> {
                connection.close();
                connection.disconnect();

                server.close(x -> {
                    completionLatch.countDown();
                });
              });

              connection.disconnectHandler(c -> {
                connection.disconnect();
              });

              connection.sessionOpenHandler(session -> {
                session.closeHandler(x -> {
                  session.close();
                  session.free();
                });
                session.open();
              });

              connection.senderOpenHandler(sender -> {
                  send(false, connection, sender, desiredCount, bodySize, durable);
              });

              connection.receiverOpenHandler(receiver -> {
                  receive(false, connection, receiver, desiredCount, creditWindow);
              });
        });

        server.listen(port, (res) -> {
          if (!res.succeeded()) {
            System.err.println("Failed to start listening on port " + port + ":");
            res.cause().printStackTrace();
            completionLatch.countDown();
          }
        });
    }

    private static void send(final boolean client, final ProtonConnection connection, final ProtonSender sender,
                             final int desiredCount, final int bodySize, final boolean durable) {
        final StringBuilder line = new StringBuilder();
        final BufferedWriter out = getWriter();
        final AtomicInteger count = new AtomicInteger(0);
        final byte[] body = new byte[bodySize];

        Arrays.fill(body, (byte) 120);

        if(!client) {
            sender.setSource(sender.getRemoteSource());
        }

        sender.sendQueueDrainHandler((s) -> {
                try {
                    try {
                        while (!sender.sendQueueFull()) {
                            int sent = count.get();

                            if (sent > 0 && sent == desiredCount) {
                                if(client) {
                                    // Not for servers, to avoid remote-closing the 'client' connection.
                                    connection.close();
                                }
                                break;
                            }

                            final Message msg = Message.Factory.create();
                            final String id = String.valueOf(count.get());
                            final long stime = System.currentTimeMillis();
                            final Map<String, Object> props = new HashMap<>();

                            props.put("SendTime", stime);

                            msg.setMessageId(id);
                            msg.setBody(new Data(new Binary(body)));
                            msg.setApplicationProperties(new ApplicationProperties(props));

                            if (durable) {
                                msg.setDurable(true);
                            }

                            sender.send(msg);
                            count.incrementAndGet();

                            line.setLength(0);
                            out.append(line.append(id).append(',').append(stime).append('\n'));
                        }
                    } finally {
                        out.flush();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

        sender.open();
    }

    private static void receive(final boolean client, final ProtonConnection connection, final ProtonReceiver receiver,
                                final int desiredCount, final int creditWindow) {
        final StringBuilder line = new StringBuilder();
        final BufferedWriter out = getWriter();
        final AtomicInteger count = new AtomicInteger(0);
        final int creditTopUpThreshold = Math.max(1, creditWindow / 2);

        if(!client) {
            receiver.setTarget(receiver.getRemoteTarget());
        }

        receiver.setAutoAccept(false).setPrefetch(0).flow(creditWindow);
        receiver.handler((delivery, msg) -> {
                try {
                    try {
                        final Object id = msg.getMessageId();
                        final long stime = (Long) msg.getApplicationProperties().getValue().get("SendTime");
                        final long rtime = System.currentTimeMillis();

                        line.setLength(0);
                        out.append(line.append(id).append(',').append(stime).append(',').append(rtime).append('\n'));

                        delivery.disposition(ACCEPTED, true);

                        final int credit = receiver.getCredit();

                        if (credit < creditTopUpThreshold) {
                            receiver.flow(creditWindow - credit);
                        }

                        if (count.incrementAndGet() == desiredCount) {
                            if(client) {
                                // Not for servers, to avoid remote-closing the 'client' connection.
                                connection.close();
                            }
                        }
                    } finally {
                        out.flush();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

        receiver.open();
    }

    private static BufferedWriter getWriter() {
        return new BufferedWriter(new OutputStreamWriter(System.out));
    }
}
