package com.soujava.collabtime.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Hello world!
 *
 */
public class App 
{
    String [] colors = new String [] {"blue", "green", "yellow"};
    String [] names = new String [] {"George", "Max", "Eric"};
    String [] languages = new String [] {"Java", "C#", "Typescript"};

    public static void main( String[] args ) throws InterruptedException {
        new App().doit();
    }
    private final static String TOPIC = "users";

    private void doit() throws InterruptedException {

        Producer<String, User> producer = createProducer();

        while (true ) {
            Future<RecordMetadata> recordMetaData = producer.send(new ProducerRecord<>(TOPIC, "key" + random.nextInt(), generateUser()));

            try {
                System.out.println("Offset:" + recordMetaData.get().offset() +
                        " Partition:" + recordMetaData.get().partition() +
                        " Serialized Value Size:" + recordMetaData.get().serializedValueSize() +
                        " Serialized Key Size:" + recordMetaData.get().serializedKeySize() +
                        " Timestamp:" + recordMetaData.get().timestamp());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            Thread.sleep(500);

            producer.flush();
        }

        //producer.close();
    }
    private Random random = new Random(System.currentTimeMillis());


    private static Producer<String, User> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());
        props.put(
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:8081");

        return new KafkaProducer<String, User>(props);
    }
    private User generateUser() {
        String name = names[random.nextInt(0, names.length)];
        String language = languages[random.nextInt(0, languages.length)];
        String color = colors[random.nextInt(0, colors.length)];
        return User.newBuilder()
                .setName(name)
                .setFavoriteColor(color)
                .setFavouriteLanguage(language)
                .setPoints(random.nextInt(0,10))
                .build();
    }
}
