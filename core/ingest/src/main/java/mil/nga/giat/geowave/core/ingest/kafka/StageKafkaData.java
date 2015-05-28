package mil.nga.giat.geowave.core.ingest.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.log4j.Logger;

/**
 * A class to hold intermediate stage data that must be used throughout the life
 * of the Kafka stage process.
 */
public class StageKafkaData<T extends SpecificRecordBase>
{

	private final static Logger LOGGER = Logger.getLogger(StageKafkaData.class);
	private final Map<String, Producer<String, T>> cachedProducers = new HashMap<String, Producer<String, T>>();

	public StageKafkaData() {}

	public Producer<String, T> getProducer(
			final String typeName,
			final AvroFormatPlugin<?> plugin ) {
		return getProducerCreateIfNull(
				typeName,
				plugin);
	}

	private synchronized Producer<String, T> getProducerCreateIfNull(
			final String typeName,
			final AvroFormatPlugin<?> plugin ) {
		if (!cachedProducers.containsKey(typeName)) {
			final Properties props = KafkaCommandLineOptions.getProperties();
			final ProducerConfig producerConfig = new ProducerConfig(
					props);

			final Producer<String, T> producer = new Producer<String, T>(
					producerConfig);

			cachedProducers.put(
					typeName,
					producer);
		}
		return cachedProducers.get(typeName);
	}

	public synchronized void close() {
		for (final Producer<String, T> producer : cachedProducers.values()) {
			try {
				producer.close();
			}
			catch (final Exception e) {
				LOGGER.warn(
						"Unable to close kafka producer",
						e);
			}
		}
		cachedProducers.clear();
	}

}
