package edu.colorado.cires.cmg.echofish.aws.lambda.r2zerror;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import edu.colorado.cires.cmg.echofish.data.dynamo.FileInfoRecord;
import edu.colorado.cires.cmg.echofish.data.model.CruiseProcessingMessage;
import edu.colorado.cires.cmg.echofish.data.model.SnsErrorMessage;
import edu.colorado.cires.cmg.echofish.data.model.SnsErrorMessage.Record;
import edu.colorado.cires.cmg.echofish.data.model.SnsErrorMessage.RequestPayload;
import edu.colorado.cires.cmg.echofish.data.model.SnsErrorMessage.ResponsePayload;
import edu.colorado.cires.cmg.echofish.data.model.SnsErrorMessage.Sns;
import edu.colorado.cires.cmg.echofish.data.model.jackson.ObjectMapperCreator;
import edu.colorado.cires.cmg.echofish.data.sns.SnsNotifier;
import edu.colorado.cires.cmg.echofish.data.sns.SnsNotifierFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RawToZarrErrorHandlerTest {

  private static final String TABLE_NAME = "FILE_INFO";
  private static final String TOPIC_ARN = "MOCK_TOPIC";


  private AmazonDynamoDB dynamo;
  private RawToZarrErrorHandler handler;
  private SnsNotifierFactory sns;
  private DynamoDBMapper mapper;


  @BeforeEach
  public void before() throws Exception {
    System.setProperty("sqlite4java.library.path", "native-libs");
    dynamo = DynamoDBEmbedded.create().amazonDynamoDB();
    mapper = new DynamoDBMapper(dynamo);
    sns = mock(SnsNotifierFactory.class);
    handler = new RawToZarrErrorHandler(
        sns,
        dynamo,
        new RawToZarrErrorHandlerLambdaConfiguration(
            TOPIC_ARN,
            TABLE_NAME
        ));
    createTable(dynamo, TABLE_NAME, "FILE_NAME", "CRUISE_NAME");
  }

  @AfterEach
  public void after() throws Exception {
    dynamo.shutdown();
  }

  @Test
  public void testSendToAccumulator() throws Exception {

    FileInfoRecord record = new FileInfoRecord();
    record.setCruiseName("HB0707");
    record.setShipName("Henry_B._Bigelow");
    record.setSensorName("EK60");
    record.setPipelineStatus("PROCESSING");
    record.setFileName("foo");
    mapper.save(record, DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(TABLE_NAME).config());


    SnsNotifier snsNotifier = mock(SnsNotifier.class);
    when(sns.createNotifier()).thenReturn(snsNotifier);


    CruiseProcessingMessage originalMessage = new CruiseProcessingMessage();
    originalMessage.setCruiseName("HB0707");
    originalMessage.setShipName("Henry_B._Bigelow");
    originalMessage.setSensorName("EK60");
    originalMessage.setFileName("foo");

    ResponsePayload responsePayload = new ResponsePayload();
    responsePayload.setErrorMessage("something is broken");
    responsePayload.setStackTrace(Arrays.asList("it's broke", "really broke"));

    Sns requestSns = new Sns();
    requestSns.setMessage(ObjectMapperCreator.create().writeValueAsString(originalMessage));

    Record requestRecord = new Record();
    requestRecord.setSns(requestSns);

    RequestPayload requestPayload = new RequestPayload();
    requestPayload.setRecords(Collections.singletonList(requestRecord));

    SnsErrorMessage snsErrorMessage = new SnsErrorMessage();
    snsErrorMessage.setResponsePayload(responsePayload);
    snsErrorMessage.setRequestPayload(requestPayload);

    handler.handleRequest(snsErrorMessage);

    FileInfoRecord expectedRecord = new FileInfoRecord();
    expectedRecord.setCruiseName("HB0707");
    expectedRecord.setShipName("Henry_B._Bigelow");
    expectedRecord.setSensorName("EK60");
    expectedRecord.setPipelineStatus("FAILURE");
    expectedRecord.setFileName("foo");
    expectedRecord.setErrorMessage("something is broken");
    expectedRecord.setErrorDetail("it's broke\nreally broke");

    Set<FileInfoRecord> expected = Collections.singleton(expectedRecord);

    Set<FileInfoRecord> saved = mapper.scan(FileInfoRecord.class, new DynamoDBScanExpression(),
        DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(TABLE_NAME).config()).stream().collect(Collectors.toSet());

    assertEquals(new HashSet<>(expected), saved);

    verify(snsNotifier).notify(eq(TOPIC_ARN), eq(originalMessage));
  }

  private static CreateTableResult createTable(AmazonDynamoDB ddb, String tableName, String hashKeyName, String rangeKeyName) {
    List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
    attributeDefinitions.add(new AttributeDefinition(hashKeyName, ScalarAttributeType.S));
    attributeDefinitions.add(new AttributeDefinition(rangeKeyName, ScalarAttributeType.S));

    List<KeySchemaElement> ks = new ArrayList<>();
    ks.add(new KeySchemaElement(hashKeyName, KeyType.HASH));
    ks.add(new KeySchemaElement(rangeKeyName, KeyType.RANGE));

    ProvisionedThroughput provisionedthroughput = new ProvisionedThroughput(1000L, 1000L);

    CreateTableRequest request =
        new CreateTableRequest()
            .withTableName(tableName)
            .withAttributeDefinitions(attributeDefinitions)
            .withKeySchema(ks)
            .withProvisionedThroughput(provisionedthroughput);

    return ddb.createTable(request);
  }

}