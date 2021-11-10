import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.io.*;
import java.util.Properties;

/**
 * 该类是用来模仿实时数据进行发送到kafka
 *
 */

public class CarToKafka {
    public static void main(String[] args) throws IOException, InterruptedException {

        Properties props = new Properties();

        props.put("bootstrap.servers", "hadoop100:9092,hadoop101:9092,hadoop102:9092");
        //2、指定kv的序列化类
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");




        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        BufferedReader bufferedReader = new BufferedReader(new FileReader("C:\\Users\\YL\\Desktop\\智慧交通\\cars.json"));

        String line = null;
        while ((line=bufferedReader.readLine())!=null){

            ProducerRecord<String, String> cars = new ProducerRecord<>("cars", line);
            producer.send(cars);

            producer.flush();
            Thread.sleep(10);

        }


        producer.close();


    //  kafka-console-consumer.sh --zookeeper hadoop100:2181  --from-beginning  -topic cars


    }
}
