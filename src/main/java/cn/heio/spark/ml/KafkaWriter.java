package cn.heio.spark.ml;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
public class KafkaWriter implements VoidFunction<Tuple2<String, String>> {
    private String kafkaServer;

    KafkaWriter(String kafkaServer){
        this.kafkaServer = kafkaServer;
    }

    @Override
    public void call(Tuple2<String, String> tuple) throws Exception {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaServer);
        kafkaParams.put("key.serializer", StringSerializer.class);
        kafkaParams.put("value.serializer", StringSerializer.class);
        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaParams);
        producer.send(
                new ProducerRecord<>(
                        tuple._1(), "aaa", tuple._2()
                )
        );
        System.out.println(tuple._2());
        producer.close();
    }
}
