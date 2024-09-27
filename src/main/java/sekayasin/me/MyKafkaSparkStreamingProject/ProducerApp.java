package sekayasin.me.MyKafkaSparkStreamingProject;

import java.io.FileReader;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.opencsv.CSVReader;

/**
 * Kafka Producer 
 * This producer, we will simulate streaming by reading a static dataset (COVID 19 Dataset from Kaggle) and send records to a kafka topic at regular intervals.
 *  
 */
public class ProducerApp {
	
	private static final String TOPIC = "covid19-dataset";
	private static final String CSV_FILE_PATH = "resources/COVID19_Dataset/country_wise_latest.csv";
	
    public static void main(String[] args) {
    	
		// producer configs
		Properties props = new Properties();
		
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		KafkaProducer<String, String> producer = new KafkaProducer<>(props);
		
		try (CSVReader reader = new CSVReader(new FileReader(CSV_FILE_PATH))) {
			String[] nextLine; 
			
			while ((nextLine = reader.readNext()) != null) {
				// convert csv row into a string and send it to our kafka broker
				String csvLine = String.join(",", nextLine);
				
				ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, csvLine);
				
				// Now send to kafka broker
				producer.send(record);
				
				// simulate a delay to mimc real-time streaming
				Thread.sleep(1000);
			}
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {
			producer.close();
		}
		
    }
}
