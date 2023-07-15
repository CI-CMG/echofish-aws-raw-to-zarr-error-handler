package edu.colorado.cires.cmg.echofish.aws.lambda.r2zerror;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.colorado.cires.cmg.echofish.data.dynamo.FileInfoRecord;
import edu.colorado.cires.cmg.echofish.data.model.CruiseProcessingMessage;
import edu.colorado.cires.cmg.echofish.data.model.SnsErrorMessage;
import edu.colorado.cires.cmg.echofish.data.model.SnsErrorMessage.ResponsePayload;
import edu.colorado.cires.cmg.echofish.data.model.jackson.ObjectMapperCreator;
import edu.colorado.cires.cmg.echofish.data.sns.SnsNotifierFactory;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawToZarrErrorHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(RawToZarrErrorHandler.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperCreator.create();


  private final SnsNotifierFactory sns;
  private final AmazonDynamoDB client;
  private final RawToZarrErrorHandlerLambdaConfiguration configuration;

  public RawToZarrErrorHandler(SnsNotifierFactory sns, AmazonDynamoDB client, RawToZarrErrorHandlerLambdaConfiguration configuration) {
    this.sns = sns;
    this.client = client;
    this.configuration = configuration;
  }

  public void handleRequest(SnsErrorMessage message) {

    LOGGER.info("Started Event: {}", message);
    CruiseProcessingMessage originalMessage;
    try {
      originalMessage = OBJECT_MAPPER.readValue(
          message.getRequestPayload().getRecords().get(0).getSns().getMessage(),
          CruiseProcessingMessage.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    setProcessingFileStatus(originalMessage, message);
    notifyTopic(originalMessage);

    LOGGER.info("Finished Event: {}", message);
  }

  private void notifyTopic(CruiseProcessingMessage message) {
    LOGGER.info("Notifying Accumulator: {} => {}", configuration.getAccumulatorTopicArn(), message);
    sns.createNotifier().notify(configuration.getAccumulatorTopicArn(), message);
  }

  private void setProcessingFileStatus(CruiseProcessingMessage message, SnsErrorMessage snsError) {
    LOGGER.info("Updating Database: {}", message);
    DynamoDBMapper mapper = new DynamoDBMapper(client);
    FileInfoRecord record = mapper.load(
        FileInfoRecord.class,
        message.getFileName(),
        message.getCruiseName(),
        DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(configuration.getTableName()).config());
    record.setPipelineStatus("FAILURE");
    record.setErrorMessage(getErrorMessage(snsError));
    record.setErrorDetail(getErrorDetail(snsError));
    mapper.save(record, DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(configuration.getTableName()).config());
  }

  private static String getErrorMessage(SnsErrorMessage snsError) {
    String errorMessage = Optional.ofNullable(snsError.getResponsePayload()).orElseGet(ResponsePayload::new).getErrorMessage();
    if (StringUtils.isBlank(errorMessage)) {
      return "An Unknown Error Occurred";
    }
    return errorMessage.trim();
  }

  private static String getErrorDetail(SnsErrorMessage snsError) {
    List<String> stackTrace = Optional.ofNullable(snsError.getResponsePayload()).orElseGet(ResponsePayload::new).getStackTrace().stream()
        .map(String::trim)
        .collect(Collectors.toList());
    if (stackTrace.isEmpty()) {
      return getErrorMessage(snsError);
    }
    return String.join("\n", stackTrace);
  }

}
