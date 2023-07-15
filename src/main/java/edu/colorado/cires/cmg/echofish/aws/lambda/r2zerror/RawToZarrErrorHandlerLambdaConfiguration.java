package edu.colorado.cires.cmg.echofish.aws.lambda.r2zerror;

public class RawToZarrErrorHandlerLambdaConfiguration {

  private final String accumulatorTopicArn;
  private final String tableName;

  public RawToZarrErrorHandlerLambdaConfiguration(String accumulatorTopicArn, String tableName) {
    this.accumulatorTopicArn = accumulatorTopicArn;
    this.tableName = tableName;
  }


  public String getAccumulatorTopicArn() {
    return accumulatorTopicArn;
  }

  public String getTableName() {
    return tableName;
  }
}
