use crate::adsb_parser::AdsbMessage;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;
use std::env;
use std::time::Duration;

// Kafka publisher function
pub async fn publish_to_kafka(
    producer: &FutureProducer,
    message: &AdsbMessage,
) -> Result<(), KafkaError> {
    // Read the topic from the environment variable
    let topic = env::var("KAFKA_TOPIC").expect("KAFKA_TOPIC not set");

    let kafka_message = serde_json::to_string(&message).unwrap();

    let record = FutureRecord::to(&topic)
        .payload(&kafka_message)
        .key(&message.hex_ident);

    match producer.send(record, Duration::from_secs(0)).await {
        Ok(confirmation) => {
            println!(
                "Kafka: Successfully published to topic {}: {:?}",
                topic, confirmation
            );
            Ok(())
        }
        Err((err, _)) => {
            eprintln!("Kafka: Failed to publish to topic {}: {:?}", topic, err);
            Err(err)
        }
    }
}

// Initialize the Kafka producer with optional SCRAM-SHA-512 or anonymous authentication
pub fn init_kafka_producer(brokers: &str) -> FutureProducer {
    let auth_type = env::var("KAFKA_AUTH_TYPE").unwrap_or_else(|_| "SCRAM-SHA-512".to_string());
    let enable_tls = env::var("KAFKA_ENABLE_TLS").unwrap_or_else(|_| "true".to_string()) == "true";

    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000");

    // Set the security protocol based on whether TLS is enabled
    if auth_type == "Anonymous" {
        // Use "PLAINTEXT" for no TLS or "SSL" for TLS, without SASL
        let security_protocol = if enable_tls { "SSL" } else { "PLAINTEXT" };
        config.set("security.protocol", security_protocol);
        println!(
            "Kafka: Using anonymous authentication with protocol {}",
            security_protocol
        );
    } else if auth_type == "SCRAM-SHA-512" {
        // Use SCRAM-SHA-512 authentication with SASL
        let security_protocol = if enable_tls {
            "SASL_SSL"
        } else {
            "SASL_PLAINTEXT"
        };
        let kafka_username = env::var("KAFKA_USERNAME").expect("KAFKA_USERNAME not set");
        let kafka_password = env::var("KAFKA_PASSWORD").expect("KAFKA_PASSWORD not set");
        let sasl_mechanism =
            env::var("KAFKA_SASL_MECHANISM").unwrap_or_else(|_| "SCRAM-SHA-512".to_string());

        config
            .set("security.protocol", security_protocol)
            .set("sasl.mechanism", &sasl_mechanism)
            .set("sasl.username", &kafka_username)
            .set("sasl.password", &kafka_password);
        println!(
            "Kafka: Using SASL authentication with protocol {}",
            security_protocol
        );
    }

    config.create().expect("Producer creation error")
}
