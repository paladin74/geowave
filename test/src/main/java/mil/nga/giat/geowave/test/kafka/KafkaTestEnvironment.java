package mil.nga.giat.geowave.test.kafka;

import java.io.File;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import mil.nga.giat.geowave.core.cli.GeoWaveMain;
import mil.nga.giat.geowave.core.ingest.kafka.KafkaCommandLineOptions;
import mil.nga.giat.geowave.test.GeoWaveTestEnvironment;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;

abstract public class KafkaTestEnvironment extends
		GeoWaveTestEnvironment

{
	private final static Logger LOGGER = Logger.getLogger(KafkaTestEnvironment.class);

	protected static KafkaServerStartable kafkaServer;

	protected static String KAFKA_TEST_TOPIC = "gpxtesttopic";
	public static final File DEFAULT_LOG_DIR = new File(
			TEMP_DIR,
			"kafka-logs");

	protected void testKafkaStage(
			final String ingestFilePath ) {
		LOGGER.warn("Staging '" + ingestFilePath + "' to a Kafka topic - this may take several minutes...");
		String[] args = null;
		synchronized (MUTEX) {
			args = StringUtils.split(
					"-kafkastage -kafkatopic " + KAFKA_TEST_TOPIC + " -f gpx -b " + ingestFilePath,
					' ');
		}

		GeoWaveMain.main(args);
	}

	@BeforeClass
	public static void setupKafkaServer()
			throws Exception {
		final String zkConnection = miniAccumulo.getZooKeepers();
		final KafkaConfig config = getKafkaConfig(zkConnection);
		kafkaServer = new KafkaServerStartable(
				config);

		kafkaServer.startup();

		// setup producer props
		setupKafkaProducerProps(zkConnection);
	}

	private static KafkaConfig getKafkaConfig(
			final String zkConnectString ) {
		final Properties props = new Properties();
//		props.put(
//				"log.dir",
//				DEFAULT_LOG_DIR.getAbsolutePath());
		props.put(
				"zookeeper.connect",
				zkConnectString);
		props.put(
				"broker.id",
				"0");
		props.put(
				"message.max.bytes",
				"5000000");
		props.put(
				"replica.fetch.max.bytes",
				"5000000");
		return new KafkaConfig(
				props);
	}

	@AfterClass
	public static void stopKafkaServer()
			throws Exception {
		kafkaServer.shutdown();
//		DEFAULT_LOG_DIR.deleteOnExit();
	}

	private static void setupKafkaProducerProps(
			String zkConnectString ) {
		System.getProperties().put(
				"metadata.broker.list",
				"localhost:9092");
		System.getProperties().put(
				"zookeeper.connect",
				zkConnectString);
		System.getProperties().put(
				"message.max.bytes",
				"5000000");
		System.getProperties().put(
				"serializer.class",
				"mil.nga.giat.geowave.core.ingest.kafka.AvroKafkaEncoder");

		KafkaCommandLineOptions.getProperties().put(
				"metadata.broker.list",
				"localhost:9092");
		KafkaCommandLineOptions.getProperties().put(
				"zookeeper.connect",
				zkConnectString);
		KafkaCommandLineOptions.getProperties().put(
				"max.message.size",
				"5000000");
		KafkaCommandLineOptions.getProperties().put(
				"serializer.class",
				"mil.nga.giat.geowave.core.ingest.kafka.AvroKafkaEncoder");
	}

}
