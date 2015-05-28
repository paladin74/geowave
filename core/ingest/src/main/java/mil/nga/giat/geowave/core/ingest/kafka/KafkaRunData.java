package mil.nga.giat.geowave.core.ingest.kafka;

import java.util.List;

import mil.nga.giat.geowave.core.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.local.IngestRunData;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;

public class KafkaRunData extends IngestRunData
{
	
	private final IngestFormatPluginProviderSpi<?, ?> pluginProvider;

	public KafkaRunData(
			IngestFormatPluginProviderSpi<?, ?> pluginProvider,
			List<WritableDataAdapter<?>> adapters,
			DataStore dataStore ) {
		super(
				adapters,
				dataStore);
		this.pluginProvider = pluginProvider;
		
		// TODO Auto-generated constructor stub
	}

	public IngestFormatPluginProviderSpi<?, ?> getPluginProvider() {
		return pluginProvider;
	}		

}
