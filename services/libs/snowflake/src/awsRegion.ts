const crowdRegion = process.env.CROWD_SNOWFLAKE_S3_REGION

if (crowdRegion) {
  process.env.AWS_REGION ??= crowdRegion
}
