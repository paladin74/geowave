package mil.nga.giat.geowave.test.kafka;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.format.gpx.GPXConsumer;
import mil.nga.giat.geowave.format.gpx.GpxIngestPlugin;
import mil.nga.giat.geowave.format.gpx.GpxTrack;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class BasicKafkaStageIT extends
		KafkaTestBase<GpxTrack>
{
	private final static Logger LOGGER = Logger.getLogger(BasicKafkaStageIT.class);

	protected static final String ZOOKEEPER_URL = "localhost:2181";
	private static final ArrayList<GpxTrack> gpxTracksList = new ArrayList<GpxTrack>();
	private final ArrayList<GPXConsumer> gpxConsumerList = new ArrayList<GPXConsumer>();
	private final ArrayList<Geometry> geomsList = new ArrayList<Geometry>();
	private static final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
	private static Geometry savedFilter = null;

	private static ExecutorService singletonExecutor;

	@BeforeClass
	static public void setupGpxTracks()
			throws Exception {

		final File gpxInputDir = new File(
				OSM_GPX_INPUT_DIR);
		final GpxIngestPlugin gpxPlugin = new GpxIngestPlugin();
		gpxPlugin.init(gpxInputDir);

		final String[] extensions = gpxPlugin.getFileExtensionFilters();
		final Collection<File> gpxFiles = FileUtils.listFiles(
				gpxInputDir,
				extensions,
				true);

		for (final File gpxFile : gpxFiles) {
			final GpxTrack[] tracks = gpxPlugin.toAvroObjects(gpxFile);
			gpxTracksList.addAll(Arrays.asList(tracks));
			break;
		}

		savedFilter = GeometryUtils.GEOMETRY_FACTORY.createPoint(
				new Coordinate(
						123.4,
						567.8)).buffer(
				1);
	}

	private static synchronized ExecutorService getSingletonExecutorService() {
		if ((singletonExecutor == null) || singletonExecutor.isShutdown()) {
			singletonExecutor = Executors.newFixedThreadPool(3);
		}
		return singletonExecutor;
	}

	@Test
	public void testBasicIngestGpx()
			throws Exception {

		final ExecutorService executorService = getSingletonExecutorService();
		try {

			executorService.execute(new Runnable() {
				@Override
				public void run() {
					testKafkaIngest(
							IndexType.SPATIAL_VECTOR,
							OSM_GPX_INPUT_DIR);
				}
			});


			final Thread t = new Thread(){
				@Override
				public void run(){
					final Producer<String, GpxTrack> producer = setupProducer();
					try {

						for (final GpxTrack track : gpxTracksList) {
							final KeyedMessage<String, GpxTrack> gpxKafkaMessage = new KeyedMessage<String, GpxTrack>(
									KafkaTestEnvironment.KAFKA_TEST_TOPIC,
									track);
							System.out.println(track.getTimestamp());
							producer.send(gpxKafkaMessage);
						}

					}
					catch (final Exception ex) {
						ex.printStackTrace();
					}
					finally {
						System.out.println((new Date()).toString() + " closing producer...");
						producer.close();
					}
				}
			};
			t.start();



			testQuery();

		}
		finally {
			System.out.println((new Date()).toString() + " entering finally...");
			executorService.shutdown();
			try {
				System.out.println((new Date()).toString() + " awaiting termination...");
				executorService.awaitTermination(
						5,
						TimeUnit.MINUTES);
			}
			catch (final InterruptedException e) {
				LOGGER.error(
						"Error waiting for submitted jobs to complete",
						e);
			}
			// close the files
			// System.out.println((new Date()).toString() +
			// " closing producer...");
			// producer.close();
		}
	}

	private void testQuery()
			throws Exception {
		final mil.nga.giat.geowave.core.store.DataStore geowaveStore = new AccumuloDataStore(
				new AccumuloIndexStore(
						accumuloOperations),
				new AccumuloAdapterStore(
						accumuloOperations),
				new AccumuloDataStatisticsStore(
						accumuloOperations),
				accumuloOperations);


		final DistributableQuery query = new SpatialQuery(
				savedFilter);
		final CloseableIterator<?> actualResults = geowaveStore.query(
				index,
				query);

		SimpleFeature testFeature = null;
		while (actualResults.hasNext()) {
			final Object obj = actualResults.next();
			if ((testFeature == null) && (obj instanceof SimpleFeature)) {
				testFeature = (SimpleFeature) obj;
			}
		}
		actualResults.close();

		System.out.println("done with queries");
	}

//	static private ExpectedResults generateExpectedResults(
//			final ArrayList<GpxTrack> gpxTracksList ) {
//		final GpxIngestPluginUtil gpxIngestPlugin = new GpxIngestPluginUtil();
//		final ArrayList<GPXConsumer> gpxConsumerList = new ArrayList<GPXConsumer>();
//		final Set<Long> hashedCentroids = new HashSet<Long>();
//
//		for (final GpxTrack gpxTrack : gpxTracksList) {
//
//			final GPXConsumer gpxConsumer = gpxIngestPlugin.toGPXConsumer(
//					gpxTrack,
//					index.getId(),
//					null);
//			gpxConsumerList.add(gpxConsumer);
//
//			while (gpxConsumer.hasNext()) {
//				final Geometry pointGeom = (Geometry) gpxConsumer.next().getValue().getDefaultGeometry();
//				hashedCentroids.add(hashCentroid(pointGeom));
//			}
//		}
//
//		final ExpectedResults expectedResults = new ExpectedResults(
//				hashedCentroids,
//				gpxTracksList.size());
//
//		return expectedResults;
//	}

	// @Test
	// public void testBasicStageGpx()
	// throws Exception {
	// testKafkaStage(OSM_GPX_INPUT_DIR);
	// }

}
