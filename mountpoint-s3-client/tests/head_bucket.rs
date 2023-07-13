#![cfg(feature = "s3_tests")]

pub mod common;

use common::*;
use mountpoint_s3_client::{EndpointConfig, HeadBucketError, ObjectClientError, S3ClientConfig, S3CrtClient};

#[tokio::test]
async fn test_head_bucket_correct_region() {
    let endpoint_config = get_test_endpoint_config();
    let client: S3CrtClient = get_test_client(endpoint_config.clone());
    let (bucket, _) = get_test_bucket_and_prefix("test_head_bucket_correct_region");

    client
        .head_bucket(&bucket, endpoint_config)
        .await
        .expect("HeadBucket failed");
}

#[tokio::test]
async fn test_head_bucket_wrong_region() {
    let endpoint_config = EndpointConfig::new().region("ap-southeast-2");
    let client =
        S3CrtClient::new(S3ClientConfig::new().endpoint_config(endpoint_config)).expect("could not create test client");
    let (bucket, _) = get_test_bucket_and_prefix("test_head_bucket_wrong_region");
    let expected_region = get_test_region();

    let result = client
        .head_bucket(&bucket, EndpointConfig::new().region(&expected_region))
        .await;

    match result {
        Err(ObjectClientError::ServiceError(HeadBucketError::IncorrectRegion(actual_region))) => {
            assert_eq!(actual_region, expected_region, "wrong region returned")
        }
        _ => panic!("incorrect result {result:?}"),
    }
}

#[tokio::test]
async fn test_head_bucket_forbidden() {
    let endpoint_config = get_test_endpoint_config();
    let client: S3CrtClient = get_test_client(endpoint_config.clone());
    let bucket = get_test_bucket_without_permissions();

    let result = client.head_bucket(&bucket, endpoint_config).await;

    assert!(matches!(
        result,
        Err(ObjectClientError::ServiceError(HeadBucketError::PermissionDenied(_)))
    ));
}

#[tokio::test]
async fn test_head_bucket_not_found() {
    let endpoint_config = get_test_endpoint_config();
    let client: S3CrtClient = get_test_client(endpoint_config.clone());
    // Buckets are case sensitive. This bucket will use path-style access and 404.
    let bucket = "DOC-EXAMPLE-BUCKET";

    let result = client.head_bucket(bucket, endpoint_config).await;

    assert!(matches!(
        result,
        Err(ObjectClientError::ServiceError(HeadBucketError::NoSuchBucket))
    ));
}
