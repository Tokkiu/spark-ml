package cn.heio.spark.ml;

import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import java.io.IOException;
import java.util.*;
import java.util.Properties;

public class Application {

    public static void main(String[] args) throws Exception {
        if (args.length < 1){
            throw new IOException("arguments lacks");
        }

        final Properties props = Util.getProperties(args[0]);

        int durationInMills = Integer.valueOf(props.getProperty("spark.duration.mills", "1000"));
        String masterUrl = props.getProperty("spark.master","local[3]");
        SparkConf sparkConf = new SparkConf()
                .setAppName("StreamLockGenerator")
                .set("spark.executor.heartbeatInterval","100")
//                .set("")
                .setMaster(masterUrl);

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(durationInMills));

        String kafkaServer = props.getProperty("kafka.server", "localhost:9092");
        String kafkaTopic = props.getProperty("kafka.source.topic", "mls");
        String topicPre = props.getProperty("kafka.result.topic", "mlr");
        String groupName = props.getProperty("kafka.group.id", "lock");

        Map<String, Object> kafkaParams = new HashedMap();
        kafkaParams.put("bootstrap.servers", kafkaServer);
        kafkaParams.put("group.id", groupName);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("auto.offset.reset", "earliest");
        String[] topicArr = kafkaTopic.split(",");
        Collection<String> topics = Arrays.asList(topicArr);

        JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = KafkaUtils.createDirectStream(streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        kafkaStream.foreachRDD(
                rdd-> {
                    rdd.map(record -> {
                        System.out.println(record.topic());
                        if (Objects.equals(record.topic(), "tasks")){
                        // do predict
                        }
                        return new Tuple2<>(topicPre, record.value());
                    }).foreach(new KafkaWriter(kafkaServer));
                }
        );

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
