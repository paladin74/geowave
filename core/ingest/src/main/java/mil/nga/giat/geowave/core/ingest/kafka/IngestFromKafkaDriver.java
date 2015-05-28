package mil.nga.giat.geowave.core.ingest.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import mil.nga.giat.geowave.core.ingest.AbstractIngestCommandLineDriver;
import mil.nga.giat.geowave.core.ingest.AccumuloCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.avro.IngestWithAvroPlugin;
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
 * This class executes the ingestion of intermediate data from a Kafka topic
 * into GeoWave.
 * 
 * @param <AbstractSimpleFeatureIngestPlugin>
 */
public class IngestFromKafkaDriver<I, O> extends
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

				AvroFormatPlugin<?> stageToAvroPlugin = null;
				try {
					stageToAvroPlugin = pluginProvider.getAvroFormatPlugin();
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

					IngestWithAvroPlugin<?, ?> ingestWithAvroPlugin = stageToAvroPlugin.getIngestWithAvroPlugin();
					WritableDataAdapter<?>[] dataAdapters = ingestWithAvroPlugin.getDataAdapters(accumuloOptions.getVisibility());
					adapters.addAll(Arrays.asList(dataAdapters));
					IngestRunData runData = new IngestRunData(
							adapters,
							dataStore);

					consumeFromTopic(
							stageToAvroPlugin,
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

	}

	private ConsumerConnector buildKafkaConsumer() {
		final Properties props = new Properties();
		props.put(
				"zookeeper.connect",
				KafkaCommandLineOptions.getProperties().get(
						"zookeeper.connect"));
		props.put(
				"group.id",
				"0");
		props.put(
				"fetch.message.max.bytes",
				"5000000");
		final ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(
				props));

		return consumer;
	}

	protected void processMessage(
			final Object dataRecord,
			final IngestRunData ingestRunData,
			final AvroFormatPlugin<I> plugin )
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

		IngestWithAvroPlugin ingestAvroPlugin = plugin.getIngestWithAvroPlugin();
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

	public void consumeFromTopic(
			final AvroFormatPlugin stageToAvroPlugin,
			final IngestRunData ingestRunData )
			throws Exception {

		ConsumerConnector consumer = buildKafkaConsumer();
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
		for (final KafkaStream stream : streams) {
			final ConsumerIterator<byte[], byte[]> it = stream.iterator();
			while (it.hasNext()) {
				final byte[] msg = it.next().message();

				final Object[] dataRecords = stageToAvroPlugin.toAvroObjects(msg);
				if (dataRecords != null) {
					for (Object dataRecord : dataRecords) {
						processMessage(
								dataRecord,
								ingestRunData,
								stageToAvroPlugin);
					}
				}

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
