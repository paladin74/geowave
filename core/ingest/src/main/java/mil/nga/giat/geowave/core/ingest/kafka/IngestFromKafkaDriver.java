package mil.nga.giat.geowave.core.ingest.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import mil.nga.giat.geowave.core.ingest.AbstractIngestCommandLineDriver;
import mil.nga.giat.geowave.core.ingest.AccumuloCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.avro.StageToAvroPlugin;

import org.apache.avro.Schema;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * This class executes the ingestion of intermediate data from a Kafka topic
 * into GeoWave.
 * 
 * @param <AbstractSimpleFeatureIngestPlugin>
 */
public class IngestFromKafkaDriver<I> extends
		AbstractIngestCommandLineDriver
{
	private final static Logger LOGGER = Logger.getLogger(IngestFromKafkaDriver.class);
	private KafkaCommandLineOptions kafkaOptions;
	private AccumuloCommandLineOptions accumuloOptions;
	private static ExecutorService singletonExecutor;
	private static int DEFAULT_NUM_CONCURRENT_CONSUMERS = 5;

	public IngestFromKafkaDriver(
			final String operation ) {
		super(
				operation);
	}

	private synchronized ExecutorService getSingletonExecutorService() {
		if ((singletonExecutor == null) || singletonExecutor.isShutdown()) {
			singletonExecutor = Executors.newFixedThreadPool(DEFAULT_NUM_CONCURRENT_CONSUMERS);
		}
		return singletonExecutor;
	}

	private void configureAndRunPluginConsumer(
			final IngestFormatPluginProviderSpi<?, ?> pluginProvider,
			final StageToAvroPlugin stageToAvroPlugin )
			throws Exception {
		final ExecutorService executorService = getSingletonExecutorService();
		executorService.execute(new Runnable() {

			@Override
			public void run() {
				try {
					ConsumerConnector consumer = buildKafkaConsumer();
					consumeFromTopic(
							pluginProvider,
							stageToAvroPlugin,
							consumer,
							kafkaOptions.getKafkaTopic(),
							stageToAvroPlugin.getAvroSchemaForHdfsType());
				}
				catch (final Exception e) {
					LOGGER.error(
							"Error setting up Kafka consumer for topic [" + kafkaOptions.getKafkaTopic() + "]",
							e);
				}
			}
		});
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

					configureAndRunPluginConsumer(
							pluginProvider,
							stageToAvroPlugin);

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
		final Properties props = new Properties();
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
		final ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(
				props));

		return consumer;
	}

	public void consumeFromTopic(
			final IngestFormatPluginProviderSpi<?, ?> pluginProvider,
			final StageToAvroPlugin<I> stageToAvroPlugin,
			final ConsumerConnector consumer,
			final String topic,
			final Schema _schema )
			throws Exception {
		if (consumer == null) {
			throw new Exception(
					"Kafka consumer connector is null, unable to create message streams");
		}
		final Map<String, Integer> topicCount = new HashMap<>();
		topicCount.put(
				topic,
				1);
		final Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
		final List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);
		for (final KafkaStream stream : streams) {
			final ConsumerIterator<byte[], byte[]> it = stream.iterator();
			while (it.hasNext()) {
				final byte[] msg = it.next().message();
				// GenericData.Record schema = deserialize(
				// msg,
				// _schema);
				final I[] dataRecords = stageToAvroPlugin.toAvroObjects(msg);
				System.out.println("found it....");
				for(I dataRecord : dataRecords) {
//				pluginProvider.getIngestFromHdfsPlugin().ingestWithMapper().toGeoWaveData(
//						dataRecord,
//						null,
//						accumuloOptions.getVisibility());
				}
				// AbstractSimpleFeatureIngestPlugin
				// pluginProvider.getLocalFileIngestPlugin().toGeoWaveData(null,
				// pluginProvider.getStageToAvroPlugin(null, null);
				//
				// now looking to ingest directly into accumulo
				//
				// now we need to push it to geowave data
				//
				// shove datarecords into avro
			}
		}
		consumer.shutdown();

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
