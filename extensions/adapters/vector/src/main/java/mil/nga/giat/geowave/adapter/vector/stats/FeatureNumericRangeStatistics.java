package mil.nga.giat.geowave.adapter.vector.stats;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.NumericRangeDataStatistics;

import org.opengis.feature.simple.SimpleFeature;

public class FeatureNumericRangeStatistics extends
		NumericRangeDataStatistics<SimpleFeature> implements
		FeatureStatistic
{
	public static final String STATS_TYPE = "RANGE";

	protected FeatureNumericRangeStatistics() {
		super();
	}

	public FeatureNumericRangeStatistics(
			final ByteArrayId dataAdapterId,
			final String fieldName ) {
		super(
				dataAdapterId,
				composeId(
						STATS_TYPE,
						fieldName));
	}

	public static final ByteArrayId composeId(
			final String fieldName ) {
		return composeId(
				STATS_TYPE,
				fieldName);
	}

	@Override
	public String getFieldName() {
		return decomposeNameFromId(getStatisticsId());
	}

	@Override
	protected NumericRange getRange(
			final SimpleFeature entry ) {

		final Object o = entry.getAttribute(getFieldName());
		if (o == null) return null;
		final long num = ((Number) o).longValue();
		return new NumericRange(
				num,
				num);
	}

	@Override
	public DataStatistics<SimpleFeature> duplicate() {
		return new FeatureNumericRangeStatistics(
				dataAdapterId,
				getFieldName());
	}

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(
				"range[adapter=").append(
				super.getDataAdapterId().getString());
		buffer.append(
				", field=").append(
				getFieldName());
		buffer.append(
				", min=").append(
				super.getMin());
		buffer.append(
				", max=").append(
				super.getMax());
		buffer.append("]");
		return buffer.toString();
	}

	public static class FeatureNumericRangeConfig implements
			StatsConfig<SimpleFeature>
	{
		@Override
		public DataStatistics<SimpleFeature> create(
				final ByteArrayId dataAdapterId,
				final String fieldName ) {
			return new FeatureNumericRangeStatistics(
					dataAdapterId,
					fieldName);
		}
	}
}
