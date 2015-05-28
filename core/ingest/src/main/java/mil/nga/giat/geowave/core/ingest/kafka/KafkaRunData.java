package mil.nga.giat.geowave.core.ingest.kafka;

import java.util.List;

import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.local.IngestRunData;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;

public class KafkaRunData<I, O> extends
		IngestRunData
{

	private final AvroFormatPlugin<I> pluginProvider;

	public KafkaRunData(
			AvroFormatPlugin<I> pluginProvider,
			List<WritableDataAdapter<?>> adapters,
			DataStore dataStore ) {
		super(
				adapters,
				dataStore);
		this.pluginProvider = pluginProvider;

		// TODO Auto-generated constructor stub
	}

	public AvroFormatPlugin<I> getPluginProvider() {
		return pluginProvider;
	}

}
