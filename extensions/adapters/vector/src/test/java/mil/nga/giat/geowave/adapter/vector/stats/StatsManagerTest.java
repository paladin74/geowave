package mil.nga.giat.geowave.adapter.vector.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureCountMinSketchStatistics.FeatureCountMinSketchConfig;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureFixedBinNumericStatistics.FeatureFixedBinConfig;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureHyperLogLogStatistics.FeatureHyperLogLogConfig;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureNumericHistogramStatistics.FeatureNumericHistogramConfig;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.lang.ArrayUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class StatsManagerTest
{

	private SimpleFeatureType schema;
	FeatureDataAdapter dataAdapter;

	@Before
	public void setup()
			throws AccumuloException,
			AccumuloSecurityException,
			SchemaException,
			CQLException,
			ParseException {
		schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,whennot:Date,somewhere:Polygon,pid:String");
		dataAdapter = new FeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
	}

	@Test
	public void test() {
		final StatsManager statsManager = new StatsManager(
				dataAdapter,
				schema);
		final ByteArrayId[] ids = statsManager.getSupportedStatisticsIds();
		assertEquals(
				8,
				ids.length);
		assertTrue(ArrayUtils.contains(
				ids,
				FeatureNumericRangeStatistics.composeId("pop")));
		assertTrue(ArrayUtils.contains(
				ids,
				FeatureBoundingBoxStatistics.composeId("somewhere")));
		assertTrue(ArrayUtils.contains(
				ids,
				FeatureBoundingBoxStatistics.composeId("geometry")));
		assertTrue(ArrayUtils.contains(
				ids,
				FeatureTimeRangeStatistics.composeId("when")));
		assertTrue(ArrayUtils.contains(
				ids,
				FeatureTimeRangeStatistics.composeId("whennot")));

		// can each type be created uniquely
		DataStatistics<SimpleFeature> stat = statsManager.createDataStatistics(
				dataAdapter,
				FeatureBoundingBoxStatistics.composeId("somewhere"));
		assertNotNull(stat);
		assertFalse(stat == statsManager.createDataStatistics(
				dataAdapter,
				FeatureBoundingBoxStatistics.composeId("somewhere")));

		FeatureBoundingBoxStatistics newStat = new FeatureBoundingBoxStatistics();
		newStat.fromBinary(stat.toBinary());
		assertEquals(
				newStat.getMaxY(),
				((FeatureBoundingBoxStatistics) stat).getMaxY(),
				0.001);
		assertEquals(
				newStat.getFieldName(),
				((FeatureBoundingBoxStatistics) stat).getFieldName());

		stat = statsManager.createDataStatistics(
				dataAdapter,
				FeatureTimeRangeStatistics.composeId("when"));
		assertNotNull(stat);
		assertFalse(stat == statsManager.createDataStatistics(
				dataAdapter,
				FeatureTimeRangeStatistics.composeId("when")));

		stat = statsManager.createDataStatistics(
				dataAdapter,
				FeatureNumericRangeStatistics.composeId("pop"));
		assertNotNull(stat);
		assertFalse(stat == statsManager.createDataStatistics(
				dataAdapter,
				FeatureNumericRangeStatistics.composeId("pop")));

	}

	@Test
	public void forcedConfiguration()
			throws SchemaException,
			JsonGenerationException,
			JsonMappingException,
			IndexOutOfBoundsException,
			IOException {
		SimpleFeatureType schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,whennot:Date,somewhere:Polygon,pid:String");

		schema.getDescriptor(
				1).getUserData().put(
				"stats",
				toJson(new StatsConfigurationCollection(
						Arrays.asList(
								new FeatureFixedBinConfig(
										0.0,
										1.0,
										24),
								new FeatureNumericHistogramConfig()))));
		schema.getDescriptor(
				5).getUserData().put(
				"stats",
				toJson(new StatsConfigurationCollection(
						Arrays.asList(
								new FeatureCountMinSketchConfig(
										0.01,
										0.97),
								new FeatureHyperLogLogConfig(
										24)))));
		FeatureDataAdapter dataAdapter = new FeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));

		final StatsManager statsManager = new StatsManager(
				dataAdapter,
				schema);

		final ByteArrayId[] ids = statsManager.getSupportedStatisticsIds();
		assertEquals(
				9,
				ids.length);
		DataStatistics<SimpleFeature> stat = statsManager.createDataStatistics(
				dataAdapter,
				FeatureFixedBinNumericStatistics.composeId("pop"));
		assertNotNull(stat);
		stat = statsManager.createDataStatistics(
				dataAdapter,
				FeatureNumericHistogramStatistics.composeId("pop"));
		assertNotNull(stat);
		stat = statsManager.createDataStatistics(
				dataAdapter,
				FeatureHyperLogLogStatistics.composeId("pid"));
		assertNotNull(stat);
		stat = statsManager.createDataStatistics(
				dataAdapter,
				FeatureCountMinSketchStatistics.composeId("pid"));
		assertNotNull(stat);

		StatsConfigurationCollection.statsFromString(
				schema,
				StatsConfigurationCollection.toStatsString(schema));

	}

	@SuppressWarnings("deprecation")
	private String toJson(
			StatsConfigurationCollection collection )
			throws JsonGenerationException,
			JsonMappingException,
			IOException {
		ObjectMapper mapper = new ObjectMapper();
		SerializationConfig config = mapper.getSerializationConfig();
		config.disable(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS);
		return dump(mapper.writeValueAsString(collection));
	}

	private String dump(
			String value ) {
		System.out.println(value);
		return value;
	}
}
