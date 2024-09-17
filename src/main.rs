mod adsb_parser;
mod mqtt_publisher;

use colored::Colorize;
use dotenv::dotenv;
use indicatif::{ProgressBar, ProgressStyle};
use std::io::{self, BufRead, BufReader};
use std::net::TcpStream;

fn main() -> io::Result<()> {
    // Load environment variables from the .env file
    dotenv().ok();

    // Read ADS-B hostname and port from environment variables
    let adsb_host = std::env::var("ADSB_HOST").expect("ADSB_HOST not set");
    let adsb_port = std::env::var("ADSB_PORT").expect("ADSB_PORT not set");

    let adsb_address = format!("{}:{}", adsb_host, adsb_port);

    // Connect to the ADS-B stream (using environment variables)
    let stream = TcpStream::connect(&adsb_address)?;
    let reader = BufReader::new(stream);

    // Initialize MQTT client
    let mut mqtt_client = mqtt_publisher::init_mqtt_client();

    // Create a progress bar with a style
    let progress = ProgressBar::new_spinner(); // Arbitrary number of messages
    let template =
        "{spinner:.green} [{elapsed_precise:7}] {pos:>7.cyan} messages {per_sec:4} {msg}";
    progress.set_style(
        ProgressStyle::default_bar()
            .template(&template)
            .unwrap()
            .progress_chars("##-"),
    );

    // Loop through the incoming stream and process each line
    for line in reader.lines() {
        match line {
            Ok(data) => {
                // Parse the ADS-B message
                if let Some(parsed_message) = adsb_parser::parse_adsb_message(&data) {
                    // Publish the parsed message to MQTT and update the progress bar
                    mqtt_publisher::publish_to_mqtt(&mut mqtt_client, &parsed_message, &progress);
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
