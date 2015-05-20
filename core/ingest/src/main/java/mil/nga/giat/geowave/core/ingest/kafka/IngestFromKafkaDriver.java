package mil.nga.giat.geowave.core.ingest.kafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import mil.nga.giat.geowave.core.ingest.AbstractIngestCommandLineDriver;
import mil.nga.giat.geowave.core.ingest.AccumuloCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.avro.StageToAvroPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.AbstractMapReduceIngest;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * This class executes the ingestion of intermediate data from a Kafka topic
 * into GeoWave.
 */
public class IngestFromKafkaDriver<T> extends
		AbstractIngestCommandLineDriver
{
	private final static Logger LOGGER = Logger.getLogger(IngestFromKafkaDriver.class);
	private KafkaCommandLineOptions kafkaOptions;
	private AccumuloCommandLineOptions accumuloOptions;

	public IngestFromKafkaDriver(
			final String operation ) {
		super(
				operation);
	}

	@Override
	protected void runInternal(
			final String[] args,
			final List<IngestFormatPluginProviderSpi<?, ?>> pluginProviders ) {

		try {

			for (final IngestFormatPluginProviderSpi<?, ?> pluginProvider : pluginProviders) {

				StageToAvroPlugin stageToAvroPlugin = null;
				try {
					stageToAvroPlugin = pluginProvider.getStageToAvroPlugin();
					//
					// this plugin will perform the conversion from format to
					// Avro objects
					// the data is what will read from the topic and convert to
					// object
					//
					if (stageToAvroPlugin == null) {
						LOGGER.warn("Plugin provider for ingest type '" + pluginProvider.getIngestFormatName() + "' does not support ingest from HDFS");
						continue;
					}

				}
				catch (final UnsupportedOperationException e) {
					LOGGER.warn(
							"Plugin provider '" + pluginProvider.getIngestFormatName() + "' does not support ingest from HDFS",
							e);
					continue;
				}

			}
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Error in accessing HDFS file system",
					e);
		}
	}

	private ConsumerConnector buildKafkaConsumer() {
		Properties props = new Properties();
		props.put(
				"zookeeper.connect",
				KafkaCommandLineOptions.getProperties().get(
						"zookeeper.connect"));
		props.put(
				"group.id",
				KafkaCommandLineOptions.getProperties().get(
						"zookeeper.connect"));
		props.put(
				"fetch.message.max.bytes",
				"5000000");
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(
				props));

		return consumer;
	}

	public void consumeFromTopic(
			StageToAvroPlugin<T> stageToAvroPlugin,
			ConsumerConnector consumer,
			final String topic,
			final Schema _schema ) {
		Map<String, Integer> topicCount = new HashMap<>();
		topicCount.put(
				topic,
				1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
		List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);
		for (final KafkaStream stream : streams) {
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			while (it.hasNext()) {
				byte[] msg = it.next().message();
				// GenericData.Record schema = deserialize(
				// msg,
				// _schema);
				T[] dataRecords = stageToAvroPlugin.toAvroObjects(msg);
			}
		}
		if (consumer != null) {
			consumer.shutdown();
		}
	}

	@Override
	protected void parseOptionsInternal(
			final CommandLine commandLine )
			throws ParseException {
		accumuloOptions = AccumuloCommandLineOptions.parseOptions(commandLine);
		kafkaOptions = KafkaCommandLineOptions.parseOptions(commandLine);
	}

	@Override
	protected void applyOptionsInternal(
			final Options allOptions ) {
		AccumuloCommandLineOptions.applyOptions(allOptions);
		KafkaCommandLineOptions.applyOptions(allOptions);
	}

}
