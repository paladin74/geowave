package mil.nga.giat.geowave.adapter.vector.stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

/**
 * A collection of statistics configurations targeted to a specific attribute.
 * Each configuration describes how to construct a statictic for an attribute.
 * 
 */
public class StatsConfigurationCollection
{

	private List<StatsConfig<SimpleFeature>> configrationsForAttribute;

	public StatsConfigurationCollection() {

	}

	public StatsConfigurationCollection(
			List<StatsConfig<SimpleFeature>> configrationsForAttribute ) {
		this.configrationsForAttribute = configrationsForAttribute;
	}

	public List<StatsConfig<SimpleFeature>> getConfigrationsForAttribute() {
		return configrationsForAttribute;
	}

	public void setConfigrationsForAttribute(
			List<StatsConfig<SimpleFeature>> configrationsForAttribute ) {
		this.configrationsForAttribute = configrationsForAttribute;
	}

	@SuppressWarnings("deprecation")
	public static List<DataStatistics<SimpleFeature>> parse(
			final String configJson,
			final ByteArrayId dataAdapterId,
			final String fieldName )
			throws java.lang.IllegalArgumentException {

		List<DataStatistics<SimpleFeature>> statsList = new ArrayList<DataStatistics<SimpleFeature>>();

		ObjectMapper mapper = new ObjectMapper();
		SerializationConfig serializationConfig = mapper.getSerializationConfig();
		serializationConfig.disable(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS);
		StatsConfigurationCollection collection;
		try {
			collection = mapper.readValue(
					configJson,
					StatsConfigurationCollection.class);
		}
		catch (Exception e) {
			throw new IllegalArgumentException(
					"Cannot parse " + configJson + " while configuring " + fieldName,
					e);
		}
		for (StatsConfig<SimpleFeature> config : collection.configrationsForAttribute) {
			statsList.add(config.create(
					dataAdapterId,
					fieldName));
		}
		return statsList;
	}

	public static class MultipleStatsConfigurationCollection
	{
		public Map<String, StatsConfigurationCollection> attData;

		public MultipleStatsConfigurationCollection() {

		}

		public MultipleStatsConfigurationCollection(
				Map<String, StatsConfigurationCollection> attData ) {
			super();
			this.attData = attData;
		}

	}

	@SuppressWarnings("deprecation")
	public static String toStatsString(
			final SimpleFeatureType type )
			throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		SerializationConfig serializationConfig = mapper.getSerializationConfig();
		serializationConfig.disable(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS);
		final Map<String, StatsConfigurationCollection> attributeConfig = new HashMap<String, StatsConfigurationCollection>();
		for (AttributeDescriptor descriptor : type.getAttributeDescriptors()) {
			if (descriptor.getUserData().containsKey(
					"stats")) {
				try {
					attributeConfig.put(
							descriptor.getLocalName(),
							mapper.readValue(
									descriptor.getUserData().get(
											"stats").toString(),
									StatsConfigurationCollection.class));
				}
				catch (Exception e) {
					throw new IllegalArgumentException(
							"Cannot configuration for " + descriptor.getLocalName(),
							e);
				}
			}
		}
		try {
			return mapper.writeValueAsString(new MultipleStatsConfigurationCollection(
					attributeConfig));
		}
		catch (IOException e) {
			throw new IOException(
					"Cannot write configuration for " + type.getTypeName(),
					e);
		}
	}

	@SuppressWarnings("deprecation")
	public static void statsFromString(
			final SimpleFeatureType type,
			String jsonString )
			throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		SerializationConfig serializationConfig = mapper.getSerializationConfig();
		serializationConfig.disable(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS);
		final MultipleStatsConfigurationCollection attributeConfig = mapper.readValue(
				jsonString,
				MultipleStatsConfigurationCollection.class);
		for (Map.Entry<String, StatsConfigurationCollection> descriptor : attributeConfig.attData.entrySet()) {
			type.getDescriptor(
					descriptor.getKey()).getUserData().put(
					"stats",
					mapper.writeValueAsString(descriptor.getValue()));
		}
	}
}
