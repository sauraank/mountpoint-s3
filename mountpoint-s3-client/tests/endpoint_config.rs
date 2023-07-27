#![cfg(feature = "s3_tests")]

pub mod common;

use aws_sdk_s3::types::ByteStream;
use bytes::Bytes;
use common::*;
use mountpoint_s3_client::{AddressingStyle, EndpointConfig, ObjectClient, S3ClientConfig, S3CrtClient};
use test_case::test_case;

async fn run_test<F: FnOnce(&str) -> EndpointConfig>(f: F, prefix: &str, bucket: &str) {
    let sdk_client = get_test_sdk_client().await;

    // Create one object named "hello"
    let key = format!("{prefix}hello");
    let body = b"hello world!";
    sdk_client
        .put_object()
        .bucket(&bucket)
        .key(&key)
        .body(ByteStream::from(Bytes::from_static(body)))
        .send()
        .await
        .unwrap();

    let region = get_test_region();
    let endpoint_config = f(&region);
    let config = S3ClientConfig::new().endpoint_config(endpoint_config.clone());
    let client = S3CrtClient::new(config).expect("could not create test client");

    let result = client
        .get_object(&bucket, &key, None, None)
        .await
        .expect("get_object should succeed");
    check_get_result(result, None, &body[..]).await;
}

#[test_case(AddressingStyle::Automatic, "test_default_addressing_style")]
#[test_case(AddressingStyle::Path, "test_path_addressing_style")]
#[tokio::test]
async fn test_addressing_style_region(addressing_style: AddressingStyle, prefix: &str) {
    run_test(
        |region| EndpointConfig::new(region).addressing_style(addressing_style),
        &get_unique_test_prefix(prefix),
        &get_test_bucket()
    )
    .await;
}

#[cfg(feature = "fips_tests")]
#[tokio::test]
async fn test_fips_mount_option() {
    let prefix = get_unique_test_prefix("test_fips");
    run_test(|region| EndpointConfig::new(region).use_fips(true), &prefix, &get_test_bucket()).await;
}

#[test_case(AddressingStyle::Automatic)]
#[test_case(AddressingStyle::Path)]
#[tokio::test]
async fn test_addressing_style_dualstack_option(addressing_style: AddressingStyle) {
    let prefix = get_unique_test_prefix("test_dual_stack");
    run_test(
        |region| {
            EndpointConfig::new(region)
                .addressing_style(addressing_style)
                .use_dual_stack(true)
        },
        &prefix,
        &get_test_bucket(),
    )
    .await;
}

#[cfg(feature = "fips_tests")]
#[tokio::test]
async fn test_fips_dual_stack_mount_option() {
    let prefix = get_unique_test_prefix("test_fips_dual_stack");
    run_test(
        |region| EndpointConfig::new(region).use_fips(true).use_dual_stack(true),
        &prefix,
        &get_test_bucket(),
    )
    .await;
}

#[test_case(AddressingStyle::Automatic)]
#[test_case(AddressingStyle::Path)]
#[tokio::test]
async fn test_single_region_access_point(addressing_style: AddressingStyle, arn: ) {

}