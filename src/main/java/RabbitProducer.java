import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static org.apache.commons.lang3.CharEncoding.UTF_8;

public class RabbitProducer {

    private final ConnectionFactory connectionFactory;
    private final Channel channel;
    private Connection connection;

    private static final String CID_ROUTING_KEY = "cid";
    private static final String PROVISO_ROUTING_KEY = "proviso";
    private static final String ATLAS_ROUTING_KEY = "atlas";

    public static final String CID_QUEUE_NAME = "cid.queue";
    public static final String PROVISO_QUEUE_NAME = "proviso.queue";
    public static final String ATLAS_QUEUE_NAME = "atlas.queue";

    private static final String EXCHANGE_NAME = "submission.exchange";
    private static final String RETRY_EXCHANGE_NAME = "retry." + EXCHANGE_NAME;
    public static final String DLX_EXCHANGE_NAME = "dlx." + EXCHANGE_NAME;

    public static final String RETRY_CID_QUEUE_NAME = "retry." + CID_QUEUE_NAME;
    public static final String RETRY_PROVISO_QUEUE_NAME = "retry." + PROVISO_QUEUE_NAME;
    public static final String RETRY_ATLAS_QUEUE_NAME = "retry." + ATLAS_QUEUE_NAME;

    public static final String DLX_SUBMISSION_QUEUE_NAME = "dl.submission.queue";

    private Map<String,Object> buildQueueArgs(String exchangeName, String routingKey) {
        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", exchangeName);
        args.put("x-dead-letter-routing-key", "retry." + routingKey);
        return args;
    }

    public RabbitProducer() {
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");

        Map<String, Object> cidArgs = buildQueueArgs(RETRY_EXCHANGE_NAME, CID_ROUTING_KEY);
        Map<String, Object> provisoArgs = buildQueueArgs(RETRY_EXCHANGE_NAME, PROVISO_ROUTING_KEY);
        Map<String, Object> atlasArgs = buildQueueArgs(RETRY_EXCHANGE_NAME, ATLAS_ROUTING_KEY);

        Map<String, Object> retryArgs = new HashMap<>();
        retryArgs.put("x-dead-letter-exchange", EXCHANGE_NAME);
        retryArgs.put("x-message-ttl", 10000);

        try {
            connection = connectionFactory.newConnection();
            channel = connection.createChannel();

            channel.queueDeclare(CID_QUEUE_NAME, true, false, false, cidArgs);
            channel.queueDeclare(PROVISO_QUEUE_NAME, true, false, false, provisoArgs);
            channel.queueDeclare(ATLAS_QUEUE_NAME, true, false, false, atlasArgs);

            channel.queueDeclare(RETRY_CID_QUEUE_NAME, true, false, false, retryArgs);
            channel.queueDeclare(RETRY_PROVISO_QUEUE_NAME, true, false, false, retryArgs);
            channel.queueDeclare(RETRY_ATLAS_QUEUE_NAME, true, false, false, retryArgs);

            channel.queueDeclare(DLX_SUBMISSION_QUEUE_NAME, true, false, false, null);

            channel.exchangeDeclare(EXCHANGE_NAME, "direct");
            channel.exchangeDeclare(RETRY_EXCHANGE_NAME, "direct");
            channel.exchangeDeclare(DLX_EXCHANGE_NAME, "direct");

            // ALL BINDINGS
            // Normal queues bind to both normal and retry routing keys
            channel.queueBind(CID_QUEUE_NAME, EXCHANGE_NAME, CID_ROUTING_KEY);
            channel.queueBind(PROVISO_QUEUE_NAME, EXCHANGE_NAME, PROVISO_ROUTING_KEY);
            channel.queueBind(ATLAS_QUEUE_NAME, EXCHANGE_NAME, CID_ROUTING_KEY);
            channel.queueBind(ATLAS_QUEUE_NAME, EXCHANGE_NAME, PROVISO_ROUTING_KEY);
            // This is so CID and PROVISO retries dont also go to atlas again
            channel.queueBind(CID_QUEUE_NAME, EXCHANGE_NAME, "retry." + CID_ROUTING_KEY);
            channel.queueBind(PROVISO_QUEUE_NAME, EXCHANGE_NAME, "retry." + PROVISO_ROUTING_KEY);
            channel.queueBind(ATLAS_QUEUE_NAME, EXCHANGE_NAME, "retry." + ATLAS_ROUTING_KEY);
            // Retry queues just need to bind to retry name
            channel.queueBind(RETRY_CID_QUEUE_NAME, RETRY_EXCHANGE_NAME, "retry." + CID_ROUTING_KEY);
            channel.queueBind(RETRY_PROVISO_QUEUE_NAME, RETRY_EXCHANGE_NAME, "retry." + PROVISO_ROUTING_KEY);
            channel.queueBind(RETRY_ATLAS_QUEUE_NAME, RETRY_EXCHANGE_NAME, "retry." + ATLAS_ROUTING_KEY);

            channel.queueBind(DLX_SUBMISSION_QUEUE_NAME, DLX_EXCHANGE_NAME, "reject");
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void produce(String routingKey) throws IOException {
        channel.basicPublish(EXCHANGE_NAME, routingKey, null, RandomStringUtils.randomAlphabetic(20).getBytes(UTF_8));
    }

    private void close() {
        try {
            channel.close();
            connection.close();
        } catch(Exception e) {
            System.out.println("Oh well");
        }
    }

    private static Runnable createWorker(final RabbitProducer producer, final String routingKey) {
        Runnable worker = new Thread() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < 5000; i++) {
                        producer.produce(routingKey);
                        try {
                            Thread.sleep(RandomUtils.nextInt(0, 1000));
                        } catch (InterruptedException e) {
                            System.out.println("Interupted");
                        }
                    }
                } catch (IOException e) {
                    System.out.println("Shabba");
                }
            }
        };
        return worker;
    }

    public static void main(String[] args) {
        final RabbitProducer producer = new RabbitProducer();
        try {
            ExecutorService executor = Executors.newFixedThreadPool(5);

            Runnable cidWorker = createWorker(producer, CID_ROUTING_KEY);
            Runnable provisoWorker = createWorker(producer, PROVISO_ROUTING_KEY);
            executor.execute(cidWorker);
            executor.execute(provisoWorker);
            executor.shutdown();
            while (!executor.isTerminated()) {}
            executor.shutdown();
        } finally {
            producer.close();
        }
    }
}
