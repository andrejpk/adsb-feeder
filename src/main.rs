mod adsb_parser;
mod mqtt_publisher;
mod kafka_publisher;

use std::env;
use std::io::{self, BufRead, BufReader};
use std::net::TcpStream;
use dotenv::dotenv;
use indicatif::{ProgressBar, ProgressStyle};
use rdkafka::producer::FutureProducer;
use tokio::runtime::Runtime;

fn main() -> io::Result<()> {
    // Load environment variables from the .env file
    dotenv().ok();

    // Read ADS-B hostname and port from environment variables
    let adsb_host = env::var("ADSB_HOST").expect("ADSB_HOST not set");
    let adsb_port = env::var("ADSB_PORT").expect("ADSB_PORT not set");
    
    // Default to MQTT if TARGET_TYPE is not set
    let target_type = env::var("TARGET_TYPE").unwrap_or_else(|_| "MQTT".to_string());

    let adsb_address = format!("{}:{}", adsb_host, adsb_port);

    // Connect to the ADS-B stream (using environment variables)
    let stream = TcpStream::connect(&adsb_address)?;
    let reader = BufReader::new(stream);

    // Create a progress bar with a style
    let progress = ProgressBar::new_spinner();
    progress.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] {pos:>7} {per_sec} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );

    // Initialize either Kafka or MQTT client
    let mut mqtt_client = None;
    let kafka_producer: Option<FutureProducer>;

    match target_type.as_str() {
        "Kafka" => {
            let kafka_brokers = env::var("KAFKA_BROKERS").expect("KAFKA_BROKERS not set");
            kafka_producer = Some(kafka_publisher::init_kafka_producer(&kafka_brokers));
        }
        _ => {
            mqtt_client = Some(mqtt_publisher::init_mqtt_client());
            kafka_producer = None;
        }
    }

    // Initialize a Tokio runtime for Kafka async operations
    let rt = Runtime::new().unwrap();

    // Loop through the incoming stream and process each line
    for line in reader.lines() {
        match line {
            Ok(data) => {
                // Parse the ADS-B message
                if let Some(parsed_message) = adsb_parser::parse_adsb_message(&data) {
                    match target_type.as_str() {
                        "Kafka" => {
                            // Publish to Kafka
                            if let Some(producer) = &kafka_producer {
                                rt.block_on(kafka_publisher::publish_to_kafka(producer, &parsed_message)).unwrap();
                            }
                        }
                        _ => {
                            // Publish to MQTT
                            if let Some(client) = &mut mqtt_client {
                                mqtt_publisher::publish_to_mqtt(client, &parsed_message, &progress);
                            }
                        }
                    }
                } else {
                    eprintln!("Failed to parse message: {}", data);
                }
            }
            Err(e) => {
                eprintln!("Error reading from stream: {}", e);
                break;
            }
        }
    }

    progress.finish_with_message("All messages processed");

    Ok(())
}
