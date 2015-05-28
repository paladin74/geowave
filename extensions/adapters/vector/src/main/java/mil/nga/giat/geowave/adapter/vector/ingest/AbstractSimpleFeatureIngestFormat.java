package mil.nga.giat.geowave.adapter.vector.ingest;

import mil.nga.giat.geowave.core.ingest.IngestFormatOptionProvider;
import mil.nga.giat.geowave.core.ingest.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.avro.IngestFromAvroPlugin;
import mil.nga.giat.geowave.core.ingest.avro.StageToAvroPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;

import org.opengis.feature.simple.SimpleFeature;

abstract public class AbstractSimpleFeatureIngestFormat<I> implements
		IngestFormatPluginProviderSpi<I, SimpleFeature>
{
	protected final CQLFilterOptionProvider cqlFilterOptionProvider = new CQLFilterOptionProvider();
	protected AbstractSimpleFeatureIngestPlugin<I> myInstance;

	private synchronized AbstractSimpleFeatureIngestPlugin<I> getInstance() {
		if (myInstance == null) {
			myInstance = newPluginInstance();
			myInstance.setFilterProvider(cqlFilterOptionProvider);
		}
		return myInstance;
	}

	abstract protected AbstractSimpleFeatureIngestPlugin<I> newPluginInstance();

	@Override
	public StageToAvroPlugin<I> getStageToAvroPlugin() {
		return getInstance();
	}
	
//	@Override
//	public IngestFromAvroPlugin<I, ?> getIngestFromAvroPlugin() {
//		return getInstance();
//	}
//	
//	abstract protected IngestFromAvroPlugin<I, ?> getIngestFromAvroPlugin()
//			throws UnsupportedOperationException;

	@Override
	public IngestFromHdfsPlugin<I, SimpleFeature> getIngestFromHdfsPlugin() {
		return getInstance();
	}

	@Override
	public LocalFileIngestPlugin<SimpleFeature> getLocalFileIngestPlugin() {
		return getInstance();
	}

	@Override
	public IngestFormatOptionProvider getIngestFormatOptionProvider() {
		return cqlFilterOptionProvider;
	}

}
