package mil.nga.giat.geowave.adapter.vector.stats;

import java.nio.ByteBuffer;
import java.text.MessageFormat;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;

import org.opengis.feature.simple.SimpleFeature;

public class FeatureFixedBinNumericStatistics extends
		AbstractDataStatistics<SimpleFeature> implements
		FeatureStatistic
{
	public static final String STATS_TYPE = "ATT_BIN";
	private long count[] = new long[23];
	private long totalCount = 0;
	private double minValue = Double.MAX_VALUE;
	private double maxValue = Double.MIN_VALUE;

	protected FeatureFixedBinNumericStatistics() {
		super();
	}

	public FeatureFixedBinNumericStatistics(
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
	public DataStatistics<SimpleFeature> duplicate() {
		return new FeatureFixedBinNumericStatistics(
				dataAdapterId,
				getFieldName());
	}

	public double[] quantile(
			final int bins ) {
		final double[] result = new double[bins];
		final double binSize = 1.0 / bins;
		for (int bin = 0; bin < bins; bin++) {
			result[bin] = quantile(binSize * (bin + 1));
		}
		return result;
	}

	public double cdf(
			final double val ) {
		final double range = maxValue - minValue;
		// one value
		if (range <= 0.0) {
			return clip(val - minValue);
		}

		final int bin = Math.min(
				(int) Math.floor((((val - minValue) / range) * count.length)),
				count.length - 1);
		if ((bin + 1) == count.length) {
			return 1.0;
		}

		double c = 0;
		final double perBinSize = binSize();
		for (int i = 0; i < bin; i++) {
			c += count[i];
		}
		final double percentageOfLastBin = (val - ((perBinSize * (bin)) + minValue)) / perBinSize;
		c += (percentageOfLastBin * count[bin + 1]);
		return c / totalCount;
	}

	private double clip(
			final double p ) {
		return p < 0 ? 0.0 : (p > 1.0 ? 1.0 : p);
	}

	private double binSize() {
		final double v = (maxValue - minValue) / count.length;
		return (v == 0.0) ? 1.0 : v;
	}

	public double quantile(
			final double percentage ) {
		final double fractionOfTotal = percentage * totalCount;
		double countThisFar = 0;
		int bin = 0;

		for (; (bin < count.length) && (countThisFar < fractionOfTotal); bin++) {
			countThisFar += count[bin];
		}
		if (bin == 0) {
			return minValue;
		}
		final double perBinSize = binSize();
		final double countUptoLastBin = countThisFar - count[bin - 1];
		return minValue + ((perBinSize * bin) + (perBinSize * ((fractionOfTotal - countUptoLastBin) / count[bin - 1])));
	}

	public double percentPopulationOverRange(
			final double start,
			final double stop ) {
		return cdf(stop) - cdf(start);
	}

	public long totalSampleSize() {
		return totalCount;
	}

	public long[] count(
			final int binSize ) {
		return count;
	}

	@Override
	public void merge(
			final Mergeable mergeable ) {
		if (mergeable instanceof FeatureFixedBinNumericStatistics) {
			final FeatureFixedBinNumericStatistics tobeMerged = (FeatureFixedBinNumericStatistics) mergeable;
			final double newMinValue = Math.min(
					minValue,
					tobeMerged.minValue);
			final double newMaxValue = Math.max(
					maxValue,
					tobeMerged.maxValue);
			this.redistribute(
					newMinValue,
					newMaxValue);
			tobeMerged.redistribute(
					newMinValue,
					newMaxValue);
			for (int i = 0; i < count.length; i++) {
				count[i] += tobeMerged.count[i];
			}

			maxValue = newMaxValue;
			minValue = newMinValue;
			totalCount += tobeMerged.totalCount;
		}
	}

	@Override
	public byte[] toBinary() {

		final int bytesNeeded = 4 + (16 * (2 + (count.length * 2)));
		final ByteBuffer buffer = super.binaryBuffer(bytesNeeded);
		buffer.putLong(totalCount);
		buffer.putDouble(minValue);
		buffer.putDouble(maxValue);
		buffer.putInt(count.length);
		for (int i = 0; i < count.length; i++) {
			buffer.putLong(count[i]);
		}
		final byte result[] = new byte[buffer.position()];
		buffer.rewind();
		buffer.get(result);
		return result;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		totalCount = buffer.getLong();
		minValue = buffer.getDouble();
		maxValue = buffer.getDouble();
		final int s = buffer.getInt();
		for (int i = 0; i < s; i++) {
			count[i] = buffer.getLong();
		}
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final SimpleFeature entry ) {
		final Object o = entry.getAttribute(getFieldName());
		if (o == null) {
			return;
		}
		final double num = ((Number) o).doubleValue();
		add(
				1,
				num);
	}

	private void add(
			final long amount,
			final double num ) {
		// entry of the the same value or first entry
		if ((totalCount == 0) || (minValue == num)) {
			count[0] += amount;
			minValue = num;
			maxValue = Math.max(
					num,
					maxValue);
		} // else if entry has a different value
		else if (minValue == maxValue) { // && num is neither
			if (num < minValue) {
				count[count.length - 1] = count[0];
				count[0] = amount;
				minValue = num;

			}
			else if (num > maxValue) {
				count[count.length - 1] = amount;
				// count[0] is unchanged
				maxValue = num;
			}
		}
		else {
			if (num < minValue) {
				redistribute(
						num,
						maxValue);
				minValue = num;

			}
			else if (num > maxValue) {
				redistribute(
						minValue,
						num);
				maxValue = num;
			}
			final double range = maxValue - minValue;
			final double b = (((num - minValue) / range) * count.length);
			final int bin = Math.min(
					(int) Math.floor(b),
					count.length - 1);
			count[bin] += amount;
		}

		totalCount += amount;
	}

	private void redistribute(
			final double newMinValue,
			final double newMaxValue ) {
		redistribute(
				new long[count.length],
				newMinValue,
				newMaxValue);
	}

	private void redistribute(
			final long[] newCount,
			final double newMinValue,
			final double newMaxValue ) {
		final double perBinSize = binSize();
		final double newRange = (newMaxValue - newMinValue);
		final double newPerBinsSize = newRange / count.length;
		double currentWindowStart = minValue;
		double currentWindowStop = minValue + perBinSize;
		for (int bin = 0; bin < count.length; bin++) {
			long distributionCount = 0;
			int destinationBin = Math.min(
					(int) Math.floor((((currentWindowStart - newMinValue) / newRange) * count.length)),
					count.length - 1);
			double destinationWindowStart = newMinValue + (destinationBin * newPerBinsSize);
			double destinationWindowStop = destinationWindowStart + newPerBinsSize;
			while (count[bin] > 0) {
				if (currentWindowStart < destinationWindowStart) {
					// take whatever is left over
					distributionCount = count[bin];
				}
				else {
					final double diff = Math.min(
							Math.max(
									currentWindowStop - destinationWindowStop,
									0.0),
							perBinSize);
					distributionCount = Math.round(count[bin] * (1.0 - (diff / perBinSize)));
				}
				newCount[destinationBin] += distributionCount;
				count[bin] -= distributionCount;

				if (destinationWindowStop < currentWindowStop) {
					destinationWindowStart = destinationWindowStop;
					destinationWindowStop += newPerBinsSize;
					destinationBin += 1;
					if ((destinationBin == count.length) && (count[bin] > 0)) {
						newCount[bin] += count[bin];
						count[bin] = 0;
					}
				}
			}

			currentWindowStart = currentWindowStop;
			currentWindowStop += perBinSize;

		}
		count = newCount;
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
		buffer.append(
				"histogram[adapter=").append(
				super.getDataAdapterId().getString());
		buffer.append(
				", field=").append(
				getFieldName());
		final MessageFormat mf = new MessageFormat(
				"{0,number,#.######}");
		buffer.append(", range={");
		buffer.append(
				mf.format(new Object[] {
					Double.valueOf(minValue)
				})).append(
				' ');
		buffer.append(mf.format(new Object[] {
			Double.valueOf(maxValue)
		}));
		buffer.append("}, bins={");
		for (final double v : this.quantile(10)) {
			buffer.append(
					mf.format(new Object[] {
						Double.valueOf(v)
					})).append(
					' ');
		}
		buffer.deleteCharAt(buffer.length() - 1);
		buffer.append("}, counts={");
		for (final long v : count(10)) {
			buffer.append(
					mf.format(new Object[] {
						Long.valueOf(v)
					})).append(
					' ');
		}
		buffer.deleteCharAt(buffer.length() - 1);
		buffer.append("}]");
		return buffer.toString();
	}
}
