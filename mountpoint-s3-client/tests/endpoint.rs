#![cfg(feature = "s3_tests")]

pub mod common;

use aws_sdk_s3::types::ByteStream;
use bytes::Bytes;
use common::*;
use mountpoint_s3_client::{AddressingStyle, Endpoint, ObjectClient, S3ClientConfig, S3CrtClient};
use test_case::test_case;

fn get_bucket_and_prefix(access_point_type: Option<AccessPointType>) -> (String, String) {
    match access_point_type {
        Some(access_point) => get_test_access_point_alias_and_prefix("test_access_point", access_point),
        None => get_test_bucket_and_prefix("test_region"),
    }
}

async fn run_test<F: FnOnce(&str) -> Endpoint>(f: F, access_point_type: Option<AccessPointType>) {
    let sdk_client = get_test_sdk_client().await;
    let (bucket, prefix) = get_bucket_and_prefix(access_point_type);

    // Create one object named "hello"
    let key = format!("{prefix}/hello");
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
    let endpoint = f(&region);
    let config = S3ClientConfig::new().endpoint(endpoint);
    let client = S3CrtClient::new(&region, config).expect("could not create test client");

    let result = client
        .get_object(&bucket, &key, None, None)
        .await
        .expect("get_object should succeed");
    check_get_result(result, None, &body[..]).await;
}

#[test_case(AddressingStyle::Automatic)]
#[test_case(AddressingStyle::Virtual)]
#[test_case(AddressingStyle::Path)]
#[tokio::test]
async fn test_addressing_style_region(addressing_style: AddressingStyle) {
    run_test(|region| Endpoint::from_region(region, addressing_style).unwrap(), None).await;
}

#[test_case(AddressingStyle::Automatic)]
#[test_case(AddressingStyle::Virtual)]
#[test_case(AddressingStyle::Path)]
#[tokio::test]
async fn test_addressing_style_uri(addressing_style: AddressingStyle) {
    run_test(
        |region| {
            let uri = format!("https://s3.{region}.amazonaws.com");
            Endpoint::from_uri(&uri, addressing_style).unwrap()
        },
        None,
    )
    .await;
}

#[test_case(AddressingStyle::Automatic)]
#[test_case(AddressingStyle::Virtual)]
#[test_case(AddressingStyle::Path)]
#[tokio::test]
async fn test_addressing_style_uri_dualstack(addressing_style: AddressingStyle) {
    run_test(
        |region| {
            let uri = format!("https://s3.dualstack.{region}.amazonaws.com");
            Endpoint::from_uri(&uri, addressing_style).unwrap()
        },
        None,
    )
    .await;
}

// FIPS endpoints can only be used with virtual-hosted-style addressing
#[test_case(AddressingStyle::Virtual)]
#[tokio::test]
async fn test_addressing_style_uri_fips(addressing_style: AddressingStyle) {
    run_test(
        |region| {
            let uri = format!("https://s3-fips.{region}.amazonaws.com");
            Endpoint::from_uri(&uri, addressing_style).unwrap()
        },
        None,
    )
    .await;
}
// FIPS endpoints can only be used with virtual-hosted-style addressing
#[test_case(AddressingStyle::Virtual)]
#[tokio::test]
async fn test_addressing_style_uri_fips_dualstack(addressing_style: AddressingStyle) {
    run_test(
        |region| {
            let uri = format!("https://s3-fips.dualstack.{region}.amazonaws.com");
            Endpoint::from_uri(&uri, addressing_style).unwrap()
        },
        None,
    )
    .await;
}

// Transfer acceleration can only be supported with virtual-hosted-style addressing
#[test_case(AddressingStyle::Virtual)]
#[tokio::test]
async fn test_addressing_style_uri_transfer_acceleration(addressing_style: AddressingStyle) {
    run_test(
        |_region| {
            let uri = "https://s3-accelerate.amazonaws.com".to_string();
            Endpoint::from_uri(&uri, addressing_style).unwrap()
        },
        None,
    )
    .await;
}

#[test_case(AddressingStyle::Automatic)]
#[test_case(AddressingStyle::Virtual)]
#[test_case(AddressingStyle::Path)]
#[tokio::test]
async fn test_single_region_access_point_alias(addressing_style: AddressingStyle) {
    run_test(
        |region| Endpoint::from_region(region, addressing_style).unwrap(),
        Some(AccessPointType::SingleRegion),
    )
    .await;
}

// For Object Labda Access Point, Lambda function needs to be configured to add API support for Put Object,
// For multi region access points, Rust SDK is not supported. Hence different helper method for these tests.
async fn run_other_access_points<F: FnOnce(&str) -> Endpoint>(f: F, access_point_type: AccessPointType) {
    let (access_point, prefix) = get_bucket_and_prefix(Some(access_point_type));

    let region = get_test_region();
    let endpoint = f(&region);
    let config = S3ClientConfig::new().endpoint(endpoint);
    let client = S3CrtClient::new(&region, config).expect("could not create test client");
    client
        .list_objects(&access_point, None, "/", 10, &prefix)
        .await
        .expect("list_object should succeed");
}

// Multi-Region Access Point does not work with Path Style addressing
// Since, MRAP alias has a '.' in it, it is not valid DNS name. So, Automatic addressing style redirect to Path style.
#[test_case(AddressingStyle::Virtual)]
#[tokio::test]
async fn test_multi_region_access_point_alias(addressing_style: AddressingStyle) {
    run_other_access_points(
        |_region| {
            let uri = "https://accesspoint.s3-global.amazonaws.com".to_string();
            Endpoint::from_uri(&uri, addressing_style).unwrap()
        },
        AccessPointType::MultiRegion,
    )
    .await;
}
// Object Lambda Access Point does not work with Path Style addressing
#[test_case(AddressingStyle::Automatic)]
#[test_case(AddressingStyle::Virtual)]
#[tokio::test]
async fn test_object_lambda_access_point_alias(addressing_style: AddressingStyle) {
    run_other_access_points(
        |region| Endpoint::from_region(region, addressing_style).unwrap(),
        AccessPointType::ObjectLambda,
    )
    .await;
}
