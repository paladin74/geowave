package mil.nga.giat.geowave.core.ingest.avro;

import mil.nga.giat.geowave.core.ingest.IngestPluginBase;

public interface IngestFromAvroPlugin<I, O> extends
		AvroPluginBase<I>,
		IngestPluginBase<I, O>
{

}
