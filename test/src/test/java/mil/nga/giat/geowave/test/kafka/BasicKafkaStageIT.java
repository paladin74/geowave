package mil.nga.giat.geowave.test.kafka;

import mil.nga.giat.geowave.core.geotime.IndexType;

import org.apache.log4j.Logger;
import org.junit.Test;

public class BasicKafkaStageIT extends
		KafkaTestBase
{
	private final static Logger LOGGER = Logger.getLogger(BasicKafkaStageIT.class);

	protected static final String ZOOKEEPER_URL = "localhost:2181";

	@Test
	public void testBasicStageGpx()
			throws Exception {

		testKafkaStage(OSM_GPX_INPUT_DIR);
	}

	// @Test
	// public void testBasicIngestGpx()
	// throws Exception {
	//
	// testKafkaIngest(
	// IndexType.SPATIAL_VECTOR,
	// OSM_GPX_INPUT_DIR);
	//
	// }
}
