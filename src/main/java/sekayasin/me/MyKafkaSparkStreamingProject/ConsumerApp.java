package sekayasin.me.MyKafkaSparkStreamingProject;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;


/**
 * Spark Kafka Consumer 
 * Spark Streaming to consume Data from Kafka topic - covid19-dataset
 *  
 */

public class ConsumerApp {
	
	public static void main(String[] args) throws IOException {
		
		// spark configuration
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("MyKafkaSparkStreaming");
		
		// streamingContext with a 10-second batch interval
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));
		
		// consumer configs
		Map<String, String> consumerConfigs = new HashMap<>();
		consumerConfigs.put("metadata.broker.list", "localhost:9092");
		consumerConfigs.put("group.id", "consumer-group-id");
		
		
		// Kafka topic to subscribe / consume
		//String topic = "covid19-dataset";
		Set<String> topics = new HashSet<>();
		topics.add("covid19-dataset");
		
		// HBase configurations
		Configuration hbaseConfig = HBaseConfiguration.create();
		hbaseConfig.set("hbase.zookeeper.quorum", "localhost");
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
		
		// Delete existing HBase table and create a new one
		createHBaseTable(hbaseConfig, "demo_covid_data");
		
		// Now creating a Data Stream from Kafka
		JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(
				streamingContext, 
				String.class, 
				String.class, 
				StringDecoder.class, 
				StringDecoder.class, 
				consumerConfigs, 
				topics);
				
		
		// process each batch of data from kafka broker
		// process CSV data and store into HBase
		
		/*stream.foreachRDD(rdd -> {
			rdd.foreach(record -> {
				String csvLine = record._2();
				String[] fields = csvLine.split(",");
				
				System.out.println("CSV record..: " + csvLine);
				
				for (String field: fields) {
					System.out.println("Field..: " + field);
				}
					
			});
		});*/
		
		// Process CSV data and store into HBase
		stream.foreachRDD(rdd -> {
			rdd.foreachPartition(partitionOfRecords -> {
				while (partitionOfRecords.hasNext()) {
					Tuple2<String, String> record = partitionOfRecords.next();
					String csvLine = record._2();
					String[] fields = csvLine.split(",");
					
					System.out.println("CSV record..: " + csvLine);
					
					for (String field: fields) {
						System.out.println("Field..: " + field);
					}
					
					// CSV fields
					String country = fields[0];
					String confirmedCases = fields[1];
					String deaths = fields[2];
					String recovered = fields[3];
					String activeCases = fields[4];
					String newCases = fields[5];
					String newDeaths = fields[6];
					String newRecovered = fields[7];
					String deathsPerHundredCases = fields[8];
					String recoveredPerHundredCases = fields[9];
					String deathsPerHundredRecovered = fields[10];
					String confirmedLastWeek = fields[11];
					String oneWeekChange = fields[12];
					String oneWeekPercentIncrease = fields[13];
					String whoRegion = fields[14];
					
					// persist into HBase
					storeToHBase( 
							country, 
							confirmedCases, 
							deaths, 
							recovered, 
							activeCases, 
							newCases, 
							newDeaths, 
							newRecovered, 
							deathsPerHundredCases, 
							recoveredPerHundredCases, 
							deathsPerHundredRecovered, 
							confirmedLastWeek, 
							oneWeekChange, 
							oneWeekPercentIncrease, 
							whoRegion
						);			
				}
			});
		});
		
		// Start the streaming context
		streamingContext.start();
		
		// do the analysis every 30secs to find the country with the highest deaths
		streamingContext.awaitTerminationOrTimeout(30000);
		analyzeHighestCovidDeaths();
		
		// await termination
		streamingContext.awaitTermination();
					
	}
	
	// HBase Table creation  insertion -- Delete if already exists
	public static void createHBaseTable(Configuration hbaseConfig, String tableName) throws IOException {
		try (Connection connection = ConnectionFactory.createConnection(hbaseConfig)) {
			Admin admin = connection.getAdmin();
			TableName table = TableName.valueOf(tableName);
			
			// Delete if table exists
			if (admin.tableExists(table)) {
				System.out.println("HBase table exists, deleting...");
				admin.disableTable(table);
				admin.deleteTable(table);
				System.out.println("HBase table deleted..: " + tableName);
			}
			
			// create a new table
			System.out.println("Creating new HBase table..");
			HTableDescriptor tableDescriptor = new HTableDescriptor(table);
			
			// Add column family
			tableDescriptor.addFamily(new HColumnDescriptor("covid19data"));
			
			admin.createTable(tableDescriptor);
			System.out.println("HBase table created...: " + tableName);
		}
	}
	
	// Store data into HBase
	// Country/Region,Confirmed,Deaths,Recovered,Active,New cases,New deaths,New recovered,Deaths / 100 Cases,Recovered / 100 Cases,
	// Deaths / 100 Recovered,Confirmed last week,1 week change,1 week % increase,WHO Region
	public static void storeToHBase( 
			String country,
			String confirmedCases,
			String deaths,
			String recovered,
			String activeCases,
			String newCases,
			String newDeaths,
			String newRecovered,
			String deathsPerHundredCases,
			String recoveredPerHundredCases,
			String deathsPerHundredRecovered,
			String confirmedLastWeek,
			String oneWeekChange,
			String oneWeekPercentIncrease,
			String whoRegion) {
		
		// HBase configurations
		Configuration hbaseConfig = HBaseConfiguration.create();
		hbaseConfig.set("hbase.zookeeper.quorum", "localhost");
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
		
		System.out.println("Starting to insert data.... ");
		
		try (Connection connection = ConnectionFactory.createConnection(hbaseConfig)) {
			
			Table table = connection.getTable(TableName.valueOf("demo_covid_data"));
			
			String rowKey = UUID.randomUUID().toString();
			
			Put put = new Put(Bytes.toBytes(rowKey));
			
			put.addColumn(Bytes.toBytes("covid19data"), Bytes.toBytes("Country/Region"), Bytes.toBytes(country));
			put.addColumn(Bytes.toBytes("covid19data"), Bytes.toBytes("Confirmed"), Bytes.toBytes(confirmedCases));
			put.addColumn(Bytes.toBytes("covid19data"), Bytes.toBytes("Deaths"), Bytes.toBytes(deaths));
			put.addColumn(Bytes.toBytes("covid19data"), Bytes.toBytes("Recovered"), Bytes.toBytes(recovered));
			put.addColumn(Bytes.toBytes("covid19data"), Bytes.toBytes("Active"), Bytes.toBytes(activeCases));
			put.addColumn(Bytes.toBytes("covid19data"), Bytes.toBytes("New cases"), Bytes.toBytes(newCases));
			put.addColumn(Bytes.toBytes("covid19data"), Bytes.toBytes("New deaths"), Bytes.toBytes(newDeaths));
			put.addColumn(Bytes.toBytes("covid19data"), Bytes.toBytes("New recovered"), Bytes.toBytes(newRecovered));
			put.addColumn(Bytes.toBytes("covid19data"), Bytes.toBytes("Deaths / 100 Cases"), Bytes.toBytes(deathsPerHundredCases));
			put.addColumn(Bytes.toBytes("covid19data"), Bytes.toBytes("Recovered / 100 Cases"), Bytes.toBytes(recoveredPerHundredCases));
			put.addColumn(Bytes.toBytes("covid19data"), Bytes.toBytes("Deaths / 100 Recovered"), Bytes.toBytes(deathsPerHundredRecovered));
			put.addColumn(Bytes.toBytes("covid19data"), Bytes.toBytes("Confirmed last week"), Bytes.toBytes(confirmedLastWeek));
			put.addColumn(Bytes.toBytes("covid19data"), Bytes.toBytes("1 week change"), Bytes.toBytes(oneWeekChange));
			put.addColumn(Bytes.toBytes("covid19data"), Bytes.toBytes("1 week % increase"), Bytes.toBytes(oneWeekPercentIncrease));
			put.addColumn(Bytes.toBytes("covid19data"), Bytes.toBytes("WHO Region"), Bytes.toBytes(whoRegion));
			
			table.put(put);
			
			System.out.println("Inserted row...: " + rowKey);
			
		} catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
	}
	
	// Sample Analysis - Find the country with the highest COVID deaths
	public static void analyzeHighestCovidDeaths() {
		
		// HBase configurations
		Configuration hbaseConfig = HBaseConfiguration.create();
		hbaseConfig.set("hbase.zookeeper.quorum", "localhost");
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
		
		try (Connection connection = ConnectionFactory.createConnection(hbaseConfig)) {
			Table table = connection.getTable(TableName.valueOf("demo_covid_data"));
			
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes("covid19data"));
			
			ResultScanner scanner = table.getScanner(scan);
			
			String countryWithMaxDeaths = "";
			int maxDeaths = 0;
			
			for (Result result: scanner) {
				String country = Bytes.toString(result.getValue(Bytes.toBytes("covid19data"), Bytes.toBytes("Country/Region")));
				String deathsStr = Bytes.toString(result.getValue(Bytes.toBytes("covid19data"), Bytes.toBytes("Deaths")));
				
				int deaths = Integer.parseInt(deathsStr);
				if (deaths > maxDeaths) {
					maxDeaths = deaths;
					countryWithMaxDeaths = country;
				}
			}
			
			System.out.println("Country with the highest COVID deaths..:" + countryWithMaxDeaths + " with " + maxDeaths + " deaths");
			
		} catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	

}
