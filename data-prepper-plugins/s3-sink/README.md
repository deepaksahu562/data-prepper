# S3 Sink

This is the Data Prepper S3 sink plugin that sends records to an S3 bucket via S3Client.

The S3 sink plugin supports OpenSearch 2.0.0 and greater.

## Usages

The s3 sink should be configured as part of Data Prepper pipeline yaml file.

## Configuration Options

```
pipeline:
  ...
  sink:
    - s3:
        aws:
          region: us-east-1
          sts_role_arn: arn:aws:iam::123456789012:role/Data-Prepper
          sts_header_overrides:
        max_retries: 5
        bucket:
          name: bucket_name
          object_key:
            path_prefix: my-elb/%{yyyy}/%{MM}/%{dd}/
            name_pattern: my-elb-%{yyyy-MM-dd'T'hh-mm-ss}.${extension}
        threshold:
          event_count: 2000
          maximum_size: 50mb
          event_collect: 15s
        codec:
          ndjson:
```

## Configuration

- `aws_region` (Optional) : The AWS region to use for credentials. Defaults to [standard SDK behavior to determine the region](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html). Defaults to `none`.

- `aws_sts_role_arn` (Optional) : The AWS STS role to assume for requests to SQS and S3. Defaults to null, which will use the [standard SDK behavior for credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html). Defaults to `none`.

- `aws_sts_header_overrides`  (Optional) : An optional map of header overrides to make when assuming the IAM role for the sink plugin. Defaults to `none`.

- `max_retries` (Optional) : An integer value indicates the maximum number of times that single request should be retired in-order to ingest data to amazon s3. Defaults to `5`.

- `bucket` (Required) : Object storage built to store and retrieve any amount of data from anywhere, User must provide bucket name.

- `object_key` (Optional) : It contains `path_prefix` and `file_pattern`. Defaults to s3 object `events-%{yyyy-MM-dd'T'hh-mm-ss}` inside bucket root directory.

- `path_prefix` (Optional) : path_prefix nothing but directory structure inside bucket in-order to store objects. Defaults to `none`.

- `name_pattern` (Optional) : s3-bucket object name will be created based on following pattern (%{USER-DEFINED-PORTION}-%{EPOCH_SECONDS}-${RANDOM}.%{EXTENSION}). `json` is the default extension, If user is not providing any extension. Defaults to `events-%{yyyy-MM-dd'T'hh-mm-ss}.json`.

- `event_count` (Required) : An integer value indicates the maximum number of events required to ingest into s3-bucket as part of threshold.

- `maximum_size` (Optional) : A String representing the count or size of bytes required to ingest into s3-bucket as part of threshold. Defaults to `50mb`.

- `event_collect` (Required) : A String representing how long events should be collected before ingest into s3-bucket as part of threshold. All Duration values are a string that represents a duration. They support ISO_8601 notation string ("PT20.345S", "PT15M", etc.) as well as simple notation Strings for seconds ("60s") and milliseconds ("1500ms").

- `buffer_type` (Optional) : Records stored temporary before flushing into s3 bucket. Possible values are `local_file` and `in_memory`. Defaults to `in_memory`.


## Developer Guide

This plugin is compatible with Java 8. See

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)