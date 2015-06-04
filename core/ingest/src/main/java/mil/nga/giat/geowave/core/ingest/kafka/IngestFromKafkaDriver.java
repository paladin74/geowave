package mil.nga.giat.geowave.core.ingest.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
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
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.IngestPluginBase;
import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.avro.GenericAvroSerializer;
import mil.nga.giat.geowave.core.ingest.local.IngestRunData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * /** This class executes the ingestion of intermediate data from a Kafka topic
 * into GeoWave.
 * 
 * @param <I>
 * @param <O>
 */
public class IngestFromKafkaDriver<I, O> extends
		AbstractIngestCommandLineDriver
{
	private final static Logger LOGGER = Logger.getLogger(IngestFromKafkaDriver.class);
	private static int NUM_CONCURRENT_CONSUMERS = KafkaCommandLineOptions.DEFAULT_NUM_CONCURRENT_CONSUMERS;
	private final static int DAYS_TO_AWAIT_COMPLETION = 999;
	private KafkaCommandLineOptions kafkaOptions;
	private AccumuloCommandLineOptions accumuloOptions;
	private final GenericAvroSerializer<I> deserializer;
	private static ExecutorService singletonExecutor;

	public IngestFromKafkaDriver(
			final String operation ) {
		super(
				operation);
		deserializer = new GenericAvroSerializer<I>();
	}

	public static synchronized ExecutorService getSingletonExecutorService() {
		if ((singletonExecutor == null) || singletonExecutor.isShutdown()) {
			singletonExecutor = Executors.newFixedThreadPool(NUM_CONCURRENT_CONSUMERS);
		}
		return singletonExecutor;
	}

	public static synchronized List<Runnable> stopExecutorService() {
		if (singletonExecutor != null) {
			return singletonExecutor.shutdownNow();
		}
		return null;
	}

	@Override
	protected void runInternal(
			final String[] args,
			final List<IngestFormatPluginProviderSpi<?, ?>> pluginProviders ) {

		AccumuloOperations operations;
		try {
			operations = accumuloOptions.getAccumuloOperations();

		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.fatal(
					"Unable to connect to Accumulo with the specified options",
					e);
			return;
		}

		final DataStore dataStore = new AccumuloDataStore(
				operations);
		try {

			for (final IngestFormatPluginProviderSpi<?, ?> pluginProvider : pluginProviders) {
				final List<WritableDataAdapter<?>> adapters = new ArrayList<WritableDataAdapter<?>>();

				AvroFormatPlugin<?, ?> avroFormatPlugin = null;
				try {
					avroFormatPlugin = pluginProvider.getAvroFormatPlugin();
					if (avroFormatPlugin == null) {
						LOGGER.warn("Plugin provider for ingest type '" + pluginProvider.getIngestFormatName() + "' does not support ingest from HDFS");
						continue;
					}

					final IngestPluginBase<?, ?> ingestWithAvroPlugin = avroFormatPlugin.getIngestWithAvroPlugin();
					final WritableDataAdapter<?>[] dataAdapters = ingestWithAvroPlugin.getDataAdapters(accumuloOptions.getVisibility());
					adapters.addAll(Arrays.asList(dataAdapters));
					final IngestRunData runData = new IngestRunData(
							adapters,
							dataStore);

					launchTopicConsumer(
							avroFormatPlugin,
							runData);

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
					"Error in accessing Kafka stream",
					e);
		}
		finally {
			final ExecutorService executorService = getSingletonExecutorService();
			executorService.shutdown();
			try {
				executorService.awaitTermination(
						DAYS_TO_AWAIT_COMPLETION,
						TimeUnit.DAYS);
			}
			catch (final InterruptedException e) {
				LOGGER.error(
						"Error waiting for submitted jobs to complete",
						e);
			}
		}
	}

	private ConsumerConnector buildKafkaConsumer() {
		final Properties properties = new Properties();
		properties.put(
				"zookeeper.connect",
				KafkaCommandLineOptions.getProperties().get(
						"zookeeper.connect"));
		properties.put(
				"group.id",
				"0");

		properties.put(
				"fetch.message.max.bytes",
				KafkaCommandLineOptions.MAX_MESSAGE_FETCH_SIZE);

		// setKafkaProperty(
		// "fetch.message.max.bytes",
		// properties,
		// KafkaCommandLineOptions.MAX_MESSAGE_FETCH_SIZE);
		// setKafkaProperty(
		// "consumer.timeout.ms",
		// properties);
		// setKafkaProperty(
		// "socket.timeout.ms",
		// properties);

		final ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(
				properties));

		return consumer;
	}

	private void launchTopicConsumer(
			final AvroFormatPlugin avroFormatPlugin,
			final IngestRunData ingestRunData )
			throws Exception {
		final ExecutorService executorService = getSingletonExecutorService();
		executorService.execute(new Runnable() {

			@Override
			public void run() {
				try {
					consumeFromTopic(
							avroFormatPlugin,
							ingestRunData);
				}
				catch (final Exception e) {
					LOGGER.error(
							"Error launching Kafka topic consumer for [" + kafkaOptions.getKafkaTopic() + "]",
							e);
				}
			}
		});
	}

	public void consumeFromTopic(
			final AvroFormatPlugin avroFormatPlugin,
			final IngestRunData ingestRunData )
			throws Exception {

		final ConsumerConnector consumer = buildKafkaConsumer();
		if (consumer == null) {
			throw new Exception(
					"Kafka consumer connector is null, unable to create message streams");
		}
		final Map<String, Integer> topicCount = new HashMap<>();
		topicCount.put(
				kafkaOptions.getKafkaTopic(),
				1);

		final Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
		final List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(kafkaOptions.getKafkaTopic());
		System.out.println("listenting...");
		int counter = 1;
		for (final KafkaStream stream : streams) {
			final ConsumerIterator<byte[], byte[]> it = stream.iterator();
			while (it.hasNext()) {
				try {
					// System.out.println((new Date()).toString() +
					// " got message " + counter);
					final byte[] msg = it.next().message();
					counter++;

					final I dataRecord = deserializer.deserialize(
							msg,
							avroFormatPlugin.getAvroSchema());

					if (dataRecord != null) {
						try {
							processMessage(
									dataRecord,
									ingestRunData,
									avroFormatPlugin);
						}
						catch (Exception e) {
							LOGGER.error("Error processing message: " + e.getMessage());
						}
					}
				}
				catch (final Exception e) {
					LOGGER.error(
							"Error consuming from Kafka topic",
							e);
				}

			}
		}

		consumer.shutdown();
	}

	synchronized protected void processMessage(
			final I dataRecord,
			final IngestRunData ingestRunData,
			final AvroFormatPlugin<I, O> plugin )
			throws IOException {

		final Index supportedIndex = accumuloOptions.getIndex(plugin.getSupportedIndices());
		if (supportedIndex == null) {
			LOGGER.error("Could not get index instance, getIndex() returned null;");
			throw new IOException(
					"Could not get index instance, getIndex() returned null");
		}
		final IndexWriter indexWriter = ingestRunData.getIndexWriter(supportedIndex);
		final Index idx = indexWriter.getIndex();
		if (idx == null) {
			LOGGER.error("Could not get index instance, getIndex() returned null;");
			throw new IOException(
					"Could not get index instance, getIndex() returned null");
		}

		final IngestPluginBase<I, O> ingestAvroPlugin = plugin.getIngestWithAvroPlugin();
		try (CloseableIterator<GeoWaveData<O>> geowaveDataIt = ingestAvroPlugin.toGeoWaveData(
				dataRecord,
				idx.getId(),
				accumuloOptions.getVisibility())) {
			while (geowaveDataIt.hasNext()) {
				final GeoWaveData<O> geowaveData = geowaveDataIt.next();
				final WritableDataAdapter adapter = ingestRunData.getDataAdapter(geowaveData);
				if (adapter == null) {
					LOGGER.warn("Adapter not found for " + geowaveData.getValue());
					continue;
				}
				indexWriter.write(
						adapter,
						geowaveData.getValue());
			}
		}
	}

	@Override
	protected void parseOptionsInternal(
			final CommandLine commandLine )
			throws ParseException {
		accumuloOptions = AccumuloCommandLineOptions.parseOptions(commandLine);
		kafkaOptions = KafkaCommandLineOptions.parseOptions(commandLine);

		NUM_CONCURRENT_CONSUMERS = kafkaOptions.getKafkaNumConsumers();
	}

	@Override
	protected void applyOptionsInternal(
			final Options allOptions ) {
		AccumuloCommandLineOptions.applyOptions(allOptions);
		KafkaCommandLineOptions.applyOptions(allOptions);
	}

}
