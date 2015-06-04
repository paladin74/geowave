package mil.nga.giat.geowave.adapter.vector.stats;

import org.codehaus.jackson.annotate.JsonTypeInfo;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public interface StatsConfig<T>
{
	DataStatistics<T> create(
			final ByteArrayId dataAdapterId,
			final String fieldName );
}
