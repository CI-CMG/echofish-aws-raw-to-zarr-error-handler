package edu.colorado.cires.cmg.echofish.aws.lambda.r2zerror;

public class RawToZarrErrorHandlerLambdaConfiguration {

  private final String inputBucket;
  private final String topicArn;
  private final String tableName;

  public RawToZarrErrorHandlerLambdaConfiguration(String inputBucket, String topicArn, String tableName) {
    this.inputBucket = inputBucket;
    this.topicArn = topicArn;
    this.tableName = tableName;
  }


  public String getInputBucket() {
    return inputBucket;
  }

  public String getTopicArn() {
    return topicArn;
  }

  public String getTableName() {
    return tableName;
  }
}
