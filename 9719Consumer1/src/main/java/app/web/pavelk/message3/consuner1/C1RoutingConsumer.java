package app.web.pavelk.message3.consuner1;

import com.rabbitmq.client.*;

import java.io.IOException;

public class C1RoutingConsumer {
    private static final String EXCHANGE_TOPIC = "EXCHANGE_TOPIC";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        //--
        channel.exchangeDeclare(EXCHANGE_TOPIC, BuiltinExchangeType.TOPIC);//топик
        String[] queueName = new String[3];
        String[] routingKey = new String[]{"java", "js", "go"};


        for (int i = 0; i < queueName.length; i++) {
            AMQP.Queue.DeclareOk declareOk = channel.queueDeclare("subscriber " + i, false, false, false, null);
            queueName[i] = declareOk.getQueue();
            //.getQueue();//буфер случайное имя
            channel.queueBind(queueName[i], EXCHANGE_TOPIC, routingKey[i]);
        }

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(consumerTag + " Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");

        };

        for (int i = 0; i < queueName.length - 1; i++) {
            int finalI = i;
            Thread t = new Thread(() -> {
                System.out.println("t.start();");
                try {
                    channel.basicConsume(queueName[finalI], true, deliverCallback, consumerTag -> {
                    });//слушатель
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            t.setDaemon(true);
            t.start();
        }

        System.out.println(queueName[2] + " sub on " + routingKey[0]);
        channel.queueBind(queueName[2], EXCHANGE_TOPIC, routingKey[0]);//подписался


        Thread t = new Thread(() -> {
            System.out.println("t.start(); queueUnbind");
            try {
                Thread.sleep(7000);
                channel.queueUnbind(queueName[2], EXCHANGE_TOPIC, routingKey[0]);//отписался
                System.out.println(queueName[2] + " unsub on " + routingKey[0]);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        t.setDaemon(true);
        t.start();


        channel.basicConsume(queueName[queueName.length - 1], true, deliverCallback, (consumerTag, d) -> {
        });
    }
}
