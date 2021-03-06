package mil.nga.giat.geowave.analytic.mapreduce;

import mil.nga.giat.geowave.analytic.ConfigurationWrapper;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveConfiguratorBase;

import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobContextConfigurationWrapper implements
		ConfigurationWrapper
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(JobContextConfigurationWrapper.class);

	private final JobContext context;
	private Logger logger = LOGGER;

	public JobContextConfigurationWrapper(
			final JobContext context ) {
		super();
		this.context = context;
	}

	public JobContextConfigurationWrapper(
			final JobContext context,
			final Logger logger ) {
		super();
		this.context = context;
		this.logger = logger;
	}

	@Override
	public int getInt(
			final Enum<?> property,
			final Class<?> scope,
			final int defaultValue ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		if (context.getConfiguration().getRaw(
				propName) == null) logger.warn("Using default for property " + propName);
		int v = context.getConfiguration().getInt(
				propName,
				defaultValue);
		return v;
	}

	@Override
	public String getString(
			final Enum<?> property,
			final Class<?> scope,
			final String defaultValue ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		if (context.getConfiguration().getRaw(
				propName) == null) logger.warn("Using default for property " + propName);
		return context.getConfiguration().get(
				propName,
				defaultValue);
	}

	@Override
	public <T> T getInstance(
			final Enum<?> property,
			final Class<?> scope,
			final Class<T> iface,
			final Class<? extends T> defaultValue )
			throws InstantiationException,
			IllegalAccessException {
		try {
			final String propName = GeoWaveConfiguratorBase.enumToConfKey(
					scope,
					property);
			if (context.getConfiguration().getRaw(
					propName) == null) {
				if (defaultValue == null) return null;
				logger.warn("Using default for property " + propName);
			}
			return GeoWaveConfiguratorBase.getInstance(
					scope,
					property,
					context,
					iface,
					defaultValue);
		}
		catch (final Exception ex) {
			logger.error("Cannot instantiate " + GeoWaveConfiguratorBase.enumToConfKey(
					scope,
					property));
			throw ex;
		}
	}

	@Override
	public double getDouble(
			Enum<?> property,
			Class<?> scope,
			double defaultValue ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		if (context.getConfiguration().getRaw(
				propName) == null) logger.warn("Using default for property " + propName);
		return context.getConfiguration().getDouble(
				propName,
				defaultValue);
	}

	@Override
	public byte[] getBytes(
			Enum<?> property,
			Class<?> scope ) {
		final String propName = GeoWaveConfiguratorBase.enumToConfKey(
				scope,
				property);
		String data = context.getConfiguration().getRaw(
				propName);
		if (data == null) logger.error(propName + " not found ");
		return ByteArrayUtils.byteArrayFromString(data);
	}

}
