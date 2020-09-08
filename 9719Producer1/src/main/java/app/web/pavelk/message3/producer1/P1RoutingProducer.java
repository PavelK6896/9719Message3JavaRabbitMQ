package app.web.pavelk.message3.producer1;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.concurrent.ThreadLocalRandom;

public class P1RoutingProducer {
    private static final String EXCHANGE_TOPIC = "EXCHANGE_TOPIC";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            //--
            channel.exchangeDeclare(EXCHANGE_TOPIC, BuiltinExchangeType.TOPIC);

            String[] routingKey = new String[]{"java", "js", "go"};
            String message = "message";

            while (true) {
                int i1 = ThreadLocalRandom.current().nextInt(0, 5000);
                int i2 = ThreadLocalRandom.current().nextInt(0, 3);
                channel.basicPublish(EXCHANGE_TOPIC, routingKey[i2], null, (message + " / " + i1).getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + routingKey[i2] + "':'" + message + " / " + i1 + "'");
                Thread.sleep(i1);
            }


        }
    }
}
