import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.QueueOfferResult;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.SourceQueueWithComplete;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;

public class RabbitConsumer {

    private final ConnectionFactory connectionFactory;
    private Connection connection;

    private final ActorSystem system;
    private final Materializer materializer;

    private Map<String, Boolean> isConsumerBroken = new HashMap<>();

    private static final int MAX_RETRY_COUNT = 5;

    public RabbitConsumer() {
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        try {
            connection = connectionFactory.newConnection();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        system = ActorSystem.create("CWI");
        materializer = ActorMaterializer.create(system);
    }

    private boolean checkSwitch(String queue, MessageEnvelope messageEnvelope) {
        String s = messageEnvelope.messageBody.toLowerCase();
        if(s.contains("break")) {
            isConsumerBroken.put(queue, true);
            System.out.println("BREAKING THE CONSUMER FOR QUEUE " + queue);
            return true;
        } else if(s.contains("fix")) {
            isConsumerBroken.put(queue, false);
            System.out.println("FIXING THE CONSUMER FOR QUEUE " + queue);
            return true;
        }
        return false;
    }

    // Hideous ArrayList of Maps of objects..... NO!!!!
    private long getRetryCount(AMQP.BasicProperties properties) {
        try {
            Object x = properties.getHeaders().getOrDefault("x-death", 0);
            if(x instanceof ArrayList) {
                ArrayList headerList = (ArrayList) x;
                if(headerList.size()>0) {
                    if(headerList.get(0) instanceof Map) {
                        Map vals = (Map)headerList.get(0);
                        long count = (long)vals.getOrDefault("count", 0);
                        return count;
                    }
                }
            }
        } catch(Exception e) {
            System.out.println("Failed to output stuff... " + e.getMessage());
        }
        return 0;
    }

    private void rejectMessage(Channel channel, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        try {
            channel.basicPublish(RabbitProducer.DLX_EXCHANGE_NAME, "reject", properties, body);
            channel.basicAck(envelope.getDeliveryTag(), false);
        } catch (IOException e) {
            System.out.println("Failed to publish failed message to DLX, " +
                    "will try again on retry, will leave on original queue");
        }
    }

    private void consume(final String queueName) {
        try {
            Channel channel = connection.createChannel();
            Source<MessageEnvelope, SourceQueueWithComplete<MessageEnvelope>> queueSource = Source.<MessageEnvelope>queue(100,
                    OverflowStrategy.backpressure()).mapMaterializedValue(mat -> {
                        Consumer consumer = new DefaultConsumer(channel) {
                            @Override
                            public void handleDelivery(String consumerTag, Envelope envelope,
                                                       AMQP.BasicProperties properties, byte[] body)
                                    throws IOException {
                                String message = new String(body, "UTF-8");

                                if(getRetryCount(properties)>=MAX_RETRY_COUNT) {
                                    // move message to DLX
                                    rejectMessage(channel, envelope, properties, body);
                                } else {
                                    MessageEnvelope elem = new MessageEnvelope(envelope.getDeliveryTag(), message);

                                    // check to see if should change state of queue
                                    if (queueName.contains(envelope.getRoutingKey()) && checkSwitch(queueName, elem)) {
                                        // remove message from queue
                                        channel.basicAck(envelope.getDeliveryTag(), false);
                                    } else {
                                        CompletionStage<QueueOfferResult> offerResult = mat.offer(elem);
                                        offerResult.whenComplete((result, e) -> {
                                            if (result instanceof QueueOfferResult.Failure) {
                                                System.out.println("Oh dear something went wrong offering to the stream....");
                                            }
                                        });
                                    }
                                }
                            }
                        };
                        channel.basicConsume(queueName, consumer);
                        return mat;
                    }
            );

            Sink<MessageEnvelope, CompletionStage<Done>> consoleSink = Sink.foreach(message -> {
                System.out.println("QueueName: " + queueName + " MessageId: [" + message.messageId + "] Message: " + message.messageBody);
                if (isConsumerBroken.getOrDefault(queueName, false)) {
                    // move message onto DLX
                    System.out.println("Rejecting message on queue " + queueName);
                    channel.basicNack(message.messageId, false, false);
                } else {
                    channel.basicAck(message.messageId, false);
                }
            });
            queueSource.runWith(consoleSink, materializer);
        } catch (Exception ex) {
            System.out.println("**********************************BOOM***********************************");
            throw new RuntimeException(ex);

        }
    }

    private void close() {
        try {
            connection.close();
            system.terminate();
        } catch(Exception e) {
            System.out.println("Oh well");
        }
    }


    public static void main(String[] args) {
        RabbitConsumer consumer = new RabbitConsumer();
        try {
            consumer.consume(RabbitProducer.ATLAS_QUEUE_NAME);
            consumer.consume(RabbitProducer.CID_QUEUE_NAME);
            consumer.consume(RabbitProducer.PROVISO_QUEUE_NAME);
        } catch(Exception e) {
            System.out.println("Oh dear");
        }
    }

    class MessageEnvelope {
        private long messageId;
        private String messageBody;

        public MessageEnvelope(long messageId, String messageBody) {
            this.messageId = messageId;
            this.messageBody = messageBody;
        }
    }
}
