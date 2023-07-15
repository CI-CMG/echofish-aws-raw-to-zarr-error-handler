package edu.colorado.cires.cmg.echofish.aws.lambda.r2zerror;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.colorado.cires.cmg.echofish.data.model.SnsErrorMessage;
import edu.colorado.cires.cmg.echofish.data.model.jackson.ObjectMapperCreator;
import edu.colorado.cires.cmg.echofish.data.sns.SnsNotifierFactoryImpl;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawToZarrErrorHandlerLambda implements RequestHandler<SNSEvent, Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RawToZarrErrorHandlerLambda.class);

  static {
    Map<String, String> map = new HashMap<>();
    map.put("ACCUMULATOR_TOPIC_ARN", System.getenv("ACCUMULATOR_TOPIC_ARN"));
    map.put("TABLE_NAME", System.getenv("TABLE_NAME"));
    LOGGER.info("ENV {}", map);
  }

  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperCreator.create();
  private static final RawToZarrErrorHandler HANDLER = new RawToZarrErrorHandler(
      new SnsNotifierFactoryImpl(OBJECT_MAPPER, AmazonSNSClientBuilder.defaultClient()),
      AmazonDynamoDBClientBuilder.standard().build(),
      new RawToZarrErrorHandlerLambdaConfiguration(
          Objects.requireNonNull(System.getenv("ACCUMULATOR_TOPIC_ARN")),
          Objects.requireNonNull(System.getenv("TABLE_NAME"))));

  @Override
  public Void handleRequest(SNSEvent snsEvent, Context context) {

    LOGGER.info("Received event: {}", snsEvent);

    SnsErrorMessage snsErrorMessage;
    try {
      snsErrorMessage = OBJECT_MAPPER.readValue(snsEvent.getRecords().get(0).getSNS().getMessage(), SnsErrorMessage.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to parse SNS message", e);
    }

    HANDLER.handleRequest(snsErrorMessage);

    return null;
  }
}
