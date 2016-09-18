package cn.ryanwu.kafka.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducerExample {
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		KafkaProducerExample producer = new KafkaProducerExample();
		Future<RecordMetadata> result = producer.send("test-api-1", "message6");
		System.out.println("offset: " + result.get().offset());
	}

	public Future<RecordMetadata> send(String topic, String value) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		
		try {
			return producer.send(new ProducerRecord<String, String>(topic, value));
		}finally {
			producer.close();
		}
	}
}
