package mil.nga.giat.geowave.core.ingest.avro;

import mil.nga.giat.geowave.core.ingest.local.LocalPluginBase;
import mil.nga.giat.geowave.core.store.index.Index;

public interface AvroFormatPlugin<I> extends
		AvroPluginBase<I>,
		LocalPluginBase
{

	public IngestWithAvroPlugin<I, ?> getIngestWithAvroPlugin();

	public Index[] getSupportedIndices();

}
