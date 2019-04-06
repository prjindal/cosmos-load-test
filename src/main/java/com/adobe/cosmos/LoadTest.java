package com.adobe.cosmos;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.cosmosdb.ConnectionMode;
import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.PartitionKey;
import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import rx.Observable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTest.class);
  private AsyncDocumentClient documentClient;
  private String collectionLink = "dbs/cts-store/colls/cts-events";
  private String query = "SELECT TOP 10 * FROM t WHERE t.pnsn>%s ORDER BY t.pnsn ASC";
  private String serviceEndPoint = "https://ons-dev-va7-db.documents.azure.com:443";
  private String masterKey = System.getenv("MASTER_KEY");
  private int maxPoolSize = 1000;
  private String partitionKeySuffix;
  private int counter = 0;

  private int initialResultsToSkip = Integer.parseInt(System.getenv("INITIAL_RESULTS_TO_SKIP"));
  private long timeToSleepInMillis = Integer.parseInt(System.getenv("TIME_TO_SLEEP_IN_MILLS"));
  private long writeSleepInMills = Integer.parseInt(System.getenv("WRITE_SLEEP_MILLS"));

  private ExecutorService executorService;
  static final MetricRegistry metrics = new MetricRegistry();
  private static final Timer timer = metrics.timer("LoadTest");
  private RandomDataGenerator randomDataGenerator = new RandomDataGenerator();
  private ObjectMapper objectMapper;

  public LoadTest(final String partitionKeySuffix, final ExecutorService executorService) {
    this.partitionKeySuffix = partitionKeySuffix;
    this.documentClient = createAsyncDocumentClient();
    this.executorService = executorService;
    objectMapper = new ObjectMapper();
  }

  private void startReport() {
    final Slf4jReporter reporter = Slf4jReporter.forRegistry(metrics)
        .outputTo(LOGGER)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build();
    reporter.start(1, TimeUnit.SECONDS);
  }

  private AsyncDocumentClient createAsyncDocumentClient() {
    ConnectionPolicy connectionPolicy = ConnectionPolicy.GetDefault();
    connectionPolicy.setConnectionMode(ConnectionMode.Direct);
    connectionPolicy.setMaxPoolSize(maxPoolSize);

    AsyncDocumentClient.Builder builder = new AsyncDocumentClient.Builder();
    AsyncDocumentClient documentClient = builder.withConsistencyLevel(ConsistencyLevel.Strong)
        .withConnectionPolicy(connectionPolicy)
        .withMasterKeyOrResourceToken(
            masterKey)
        .withServiceEndpoint(serviceEndPoint).build();

    return documentClient;
  }

  private void writeRows(final List<String> partitionKeys)
      throws Exception {

    for (int i = 0; i < partitionKeys.size(); i++) {
      for (int child = 1; child <= 100; child++) {

        String key = partitionKeys.get(i);
        writeRow(createRow(key, child), key);

        key = partitionKeys.get(i) + partitionKeySuffix;
        writeRow(createRow(key, child), key);

        Thread.sleep(writeSleepInMills);
      }
    }
  }

  private void writeRow(final String document, final String partitionKey) {
    executorService.submit(() -> {
      RequestOptions requestOptions = new RequestOptions();
      requestOptions.setPartitionKey(new PartitionKey(partitionKey));
      documentClient.createDocument(
          collectionLink, new Document(document), requestOptions, false)
          .subscribe(result -> LOGGER
                  .info("Acitivity Id: {}, PartitionKey : {}", result.getActivityId(),
                      partitionKey),
              error -> LOGGER.error("Write Error :", error));
    });
  }

  private String createRow(final String partitionKey,
      final long sortKey) throws Exception {
    Row row = new Row();

    row.setChildId(UUID.randomUUID().toString());
    row.setPartitionId(partitionKey);
    row.setChildId("DIRECTORY");
    row.setChildSequenceNumber(Long.valueOf(randomDataGenerator.nextInt(1, 1999999999)));
    row.setParentOldSeqNumber(Long.valueOf(randomDataGenerator.nextInt(1, 1999999999)));
    row.setParentNewSeqNumber(sortKey);
    row.setTimestamp(Instant.now().getEpochSecond());
    row.setPayload(getDummyPayload());
    row.setIdScope(UUID.randomUUID().toString());
    row.setOperationType("CREATE");
    row.setAssetType("DIRECTORY");

    row.setNewParentId(UUID.randomUUID().toString());
    row.setNewParentOldSequenceNumber(Long.valueOf(randomDataGenerator.nextInt(1, 100000000)));
    row.setNewParentNewSequenceNumber(Long.valueOf(randomDataGenerator.nextInt(1, 100000000)));

    return objectMapper.writeValueAsString(row);
  }


  private String getDummyPayload() throws Exception {


    final Map<String, Object> result = new HashMap<>();

    final Map<String, String> head = new HashMap<>();
    head.put("seqNo", "123");
    head.put("msgType", "https://ns.adobe.com/adobecloud/event/1.0/updated");
    head.put("xRequestId", "9876");

    final Map<String, Object> body = new HashMap<>();

    final Map<String, Object> xdmMeta = new HashMap<>();
    final Map<String, String> xdmSchema = new HashMap<>();
    xdmSchema.put("name", "https://ns.adobe.com/adobecloud/core/1.0/file");
    xdmSchema.put("version", "2");
    xdmMeta.put("xdmSchema", xdmSchema);
    body.put("xdmMeta", xdmMeta);

    final Map<String, String> asset = new HashMap<>();
    asset.put("repo:id", "12345");
    asset.put("repo:parentId", "87654");
    asset.put("repo:parentSeqNo", "232");
    asset.put("repo:repositoryId", "repoId1");
    body.put("asset", asset);

    final Map<String, String> context = new HashMap<>();
    context.put("opId", "12876382");
    context.put("opType", "update");
    body.put("context", context);

    final Map<String, Object> resource = new HashMap<>();
    final Map<String, Object> resourceDetail = new HashMap<>();
    final Map<String, String> props = new HashMap<>();
    props.put("tiff:imageLength", "2248");
    props.put("tiff:imageWidth", "3264");

    final Map<String, String> changeset = new HashMap<>();
    changeset.put("tiff:imageLength", "224");
    changeset.put("tiff:imageWidth", "326");

    resourceDetail.put("generationId", "123");
    resourceDetail.put("properties", props);
    resourceDetail.put("changeset", changeset);

    resource.put("http://ns.adobe.com/adobecloud/rel/metadata/repository", resourceDetail);

    body.put("resources", resource);

    result.put("header", head);
    result.put("body", body);

    return objectMapper.writeValueAsString(result);
  }

  private void runTest(final List<String> partitionKeys) throws InterruptedException {

    for (int N = 0; N < 4; N++) {
      for (int i = 0; i < partitionKeys.size() - 9; i++) {

        getResults(partitionKeys.get(i), 1);
        getResults(partitionKeys.get(i + 1), 11);
        getResults(partitionKeys.get(i + 2), 21);

        TimeUnit.MILLISECONDS.sleep(timeToSleepInMillis);

        getResults(partitionKeys.get(i + 3), 31);
        getResults(partitionKeys.get(i + 4), 41);
        getResults(partitionKeys.get(i + 5), 51);

        TimeUnit.MILLISECONDS.sleep(timeToSleepInMillis);

        getResults(partitionKeys.get(i + 6), 61);
        getResults(partitionKeys.get(i + 7), 71);
        getResults(partitionKeys.get(i + 8), 81);

        TimeUnit.MILLISECONDS.sleep(timeToSleepInMillis);
      }
    }
  }

  private void getResults(final String partitionKey, final int sortKey) {
    fetchFromDB(partitionKey, sortKey);
    fetchFromDB(partitionKey + partitionKeySuffix, sortKey);
  }

  private void fetchFromDB(final String partitionKey, final int sortKey) {

    final String parsedQuery = String.format(query, sortKey);
    counter++;

    executorService.submit(() -> {

      // skipping few initial records
      final Timer.Context context;
      if (counter > initialResultsToSkip) {
        context = timer.time();
      } else {
        context = null;
      }

      final Observable<FeedResponse<Document>> resourceResponseObservable =
          documentClient.queryDocuments(collectionLink, parsedQuery, getFeedOptions(partitionKey));

      resourceResponseObservable.subscribe(
          feedResponse -> handleSuccess(feedResponse, context),
          error -> handleError(error, context));

    });
  }

  private void handleSuccess(FeedResponse<Document> feedResponse, Timer.Context context) {
    stopTimer(context);
    LOGGER.info("Results Size : {}  | Header : {}", feedResponse.getResults().size(),
        feedResponse.getResponseHeaders());
  }

  private void handleError(final Throwable throwable, Timer.Context context) {
    stopTimer(context);
    LOGGER.info("Error : ", throwable);
  }

  private void stopTimer(final Timer.Context context) {
    if (context != null) {
      context.stop();
    }
  }

  private FeedOptions getFeedOptions(final String partitionKeyValue) {
    final FeedOptions feedOptions = new FeedOptions();
    feedOptions.setPartitionKey(new PartitionKey(partitionKeyValue));
    feedOptions.setPopulateQueryMetrics(true);
    return feedOptions;

  }

  public static void main(String[] args) throws Exception {

    final String partitionSuffix = "_" + UUID.randomUUID().toString();
    LOGGER.info("subscriptionId :" + partitionSuffix);
    final List<String> partitionKeys = generateUniquePartitionIds();
    LOGGER.info("partitionKeys :" + partitionKeys);

    ExecutorService executorService =
        new ThreadPoolExecutor(500, 500, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>());

    final LoadTest loadTest = new LoadTest(partitionSuffix, executorService);
    // write rows into db
    loadTest.writeRows(partitionKeys);

    while (((ThreadPoolExecutor) executorService).getActiveCount() > 0) {
      TimeUnit.SECONDS.sleep(2);
    }

    TimeUnit.SECONDS.sleep(2);

    LOGGER.info("-----Write Completed-----");


    loadTest.startReport();
    // read rows
    loadTest.runTest(partitionKeys);

    while (((ThreadPoolExecutor) executorService).getActiveCount() > 0) {
      TimeUnit.SECONDS.sleep(2);
    }

    TimeUnit.SECONDS.sleep(5);

    System.exit(1);

  }

  private static List<String> generateUniquePartitionIds() {
    String totalPartitionKeys = System.getenv("TOTAL_KEYS");
    if (totalPartitionKeys == null) {
      totalPartitionKeys = "1500";
    }

    int total = Integer.parseInt(totalPartitionKeys);
    final List<String> parents = new ArrayList(total);

    while (total > 0) {
      parents.add(UUID.randomUUID().toString());
      total--;
    }
    return parents;
  }
}
