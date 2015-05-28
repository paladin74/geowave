package mil.nga.giat.geowave.core.ingest.avro;

public interface AvroFormatPlugin<I, O> extends
		AvroPluginBase<I>
{

	public StageToAvroPlugin<I> getStageToAvroPlugin();

	public IngestWithAvroPlugin<I, O> getIngestWithAvroPlugin();

}
