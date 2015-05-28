package mil.nga.giat.geowave.core.ingest.kafka;

import java.util.ArrayList;
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
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.avro.StageToAvroPlugin;
import mil.nga.giat.geowave.core.ingest.local.IngestRunData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.MemoryAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
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
public class IngestFromKafkaDriver<I, O, R> extends
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

	private <I, O> void configureAndRunPluginConsumer(
			final IngestFormatPluginProviderSpi<I, O> pluginProvider,
			final StageToAvroPlugin stageToAvroPlugin )
			throws Exception {
		final ExecutorService executorService = getSingletonExecutorService();
		// executorService.execute(new Runnable() {
		//
		// @Override
		// public void run() {
		// try {
		ConsumerConnector consumer = buildKafkaConsumer();
		consumeFromTopic(
				pluginProvider,
				stageToAvroPlugin,
				consumer,
				kafkaOptions.getKafkaTopic(),
				stageToAvroPlugin.getAvroSchema());
		// }
		// catch (final Exception e) {
		// LOGGER.error(
		// "Error setting up Kafka consumer for topic [" +
		// kafkaOptions.getKafkaTopic() + "]",
		// e);
		// }
		// }
		// });
	}

	@Override
	protected void runInternal(
			final String[] args,
			final List<IngestFormatPluginProviderSpi<?, ?>> pluginProviders ) {
		final List<WritableDataAdapter<?>> adapters = new ArrayList<WritableDataAdapter<?>>();

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
		
		IngestRunData runData = new IngestRunData(
				adapters,
				dataStore);
		
//		runData.get
	
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

	@SuppressWarnings("hiding")
	public <I, O> void consumeFromTopic(
			final IngestFormatPluginProviderSpi<I, O> pluginProvider,
			final StageToAvroPlugin<I> stageToAvroPlugin,
			final ConsumerConnector consumer,
			final String topic,
			final Schema _schema )
			throws Exception {
		if (consumer == null) {
			throw new Exception(
					"Kafka consumer connector is null, unable to create message streams");
		}
		
		final Index supportedIndex = accumuloOptions.getIndex(pluginProvider.getIngestFromHdfsPlugin().getSupportedIndices());
		
		
		final Map<String, Integer> topicCount = new HashMap<>();
		topicCount.put(
				topic,
				1);
		
		final AdapterStore adapterCache = new MemoryAdapterStore(
				pluginProvider.getIngestFromHdfsPlugin().ingestWithMapper().getDataAdapters(accumuloOptions.getVisibility()));
		
		
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
				if (dataRecords != null) {
					for (I dataRecord : dataRecords) {
						// pluginProvider.getIngestFromHdfsPlugin().getSupportedIndices()
						CloseableIterator<GeoWaveData<O>> data = pluginProvider.getIngestFromHdfsPlugin().ingestWithMapper().toGeoWaveData(
								dataRecord,
								accumuloOptions.getIndex(
										pluginProvider.getIngestFromHdfsPlugin().getSupportedIndices()).getId(),
								accumuloOptions.getVisibility());
						
						while (data.hasNext()) {
							final GeoWaveData<?> geowaveData = data.next();
//							final WritableDataAdapter[] adapters  = pluginProvider.getIngestFromHdfsPlugin().ingestWithMapper().getDataAdapters(accumuloOptions.getVisibility());
							final WritableDataAdapter<?> adapter = geowaveData.getAdapter(adapterCache);
							if (adapter == null) {
								LOGGER.warn("Adapter not found for " + geowaveData.getValue());
								continue;
							}
//							indexWriter.write(
//									adapter,
//									geowaveData.getValue());
								
//							for(WritableDataAdapter adapter :adapters ) {
//								adapter.
//							}
//							adapter.write(
//									adapter,
//									geowaveData.getValue());
//							final WritableDataAdapter adapter = ingestRunData.getDataAdapter(geowaveData);
//							if (adapter == null) {
//								LOGGER.warn("Adapter not found for " + geowaveData.getValue());
//								continue;
//							}
//							indexWriter.write(
//									adapter,
//									geowaveData.getValue());
						}
					}
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
