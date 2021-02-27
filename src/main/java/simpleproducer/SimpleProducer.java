package simpleproducer;

import java.util.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

public class SimpleProducer {

    public static String getRandom(String[] array) {
        int rnd = new Random().nextInt(array.length);
        return array[rnd];
    }

    public static void main(String[] args) {
        Scanner scan = new Scanner(System.in);
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);
        int traceId = 0;
        while(true)  {
            String name = scan.nextLine();
            String[] status = {"Delivery", "Bounce", "Complaint"};
            String textToSend = "{subject: "+name+", status:"+getRandom(status)+"}";
            traceId += 1;
//            List<Header> headers = Arrays.asList(new RecordHeader("Trace_id", String.valueOf(traceId).getBytes()));
//            producer.send(new ProducerRecord<>("kafka_tutorial", null, "Name", textToSend, headers));
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("kafka_tutorial", "Name", textToSend);
            producerRecord.headers().add("traceId", String.valueOf(traceId).getBytes());
            producer.send(producerRecord);
            if(name.equals("exit"))
                break;
        }
        producer.close();
    }
}
