package com.github.lamkenn.kafka.tutorial1;

import com.github.lamkenn.kafka.tutorial1.session.SessionTopicConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class ConsumerRecordRelayMain {
//    private static final String topic = "cable-Holdings_Combined_SEGA";
    private static final String topic = "cable-imagine_session";
    private static final String TEST_GROUP = "TestGroup1";


    public static void main(String[] args) {

//        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
//        for (int i = 0; i < 10; i++) {
//            final int index = i;
//            singleThreadExecutor.execute(new Runnable() {
//
//                @Override
//                public void run() {
//                    try {
//                        System.out.println(index);
//                        Thread.sleep(2000);
//                    } catch (InterruptedException e) {
//                        // TODO Auto-generated catch block
//                        e.printStackTrace();
//                    }
//                }
//            });
//        }

        AtomicInteger messageCounter = new AtomicInteger();

        Properties props = new Properties();
        //props.setProperty(BOOTSTRAP_SERVERS_CONFIG, "10.0.1.152:9092");
        props.setProperty(GROUP_ID_CONFIG, TEST_GROUP);
        props.setProperty(BOOTSTRAP_SERVERS_CONFIG, "beige-gurney.srvs.cloudkafka.com:9094");
        props.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
        //props.put("enable.auto.commit", "false");
        props.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty("security.protocol", "SASL_SSL");
        props.setProperty("sasl.mechanism", "SCRAM-SHA-256");

        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, "cgh6iop5", "RdGp960Erk2lRn6_-A1urWssEYpyTjIq");

        //props.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cgh6iop5\" password=\"9f9d2fd30d594880be998dfd27525678\";");
        props.setProperty("sasl.jaas.config", jaasCfg);
        Consumer<String> action = (message) -> System.out.println(message);

        SessionTopicConsumer sessionTopicConsumer = new SessionTopicConsumer("cable-imagine_session");
        new Thread(sessionTopicConsumer).start();

        //BlockingQueueConsumer<String, String> consumer = new BlockingQueueConsumer<>(topic, props, 42, action);
        //consumer.start();

    }
}
