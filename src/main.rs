mod adsb_parser;
mod mqtt_publisher;

use dotenv::dotenv;
use std::env;
use std::io::{self, BufRead, BufReader};
use std::net::TcpStream;

fn main() -> io::Result<()> {
    // Load environment variables from the .env file
    dotenv().ok();

    // Read ADS-B hostname and port from environment variables
    let adsb_host = env::var("ADSB_HOST").expect("ADSB_HOST not set");
    let adsb_port = env::var("ADSB_PORT").expect("ADSB_PORT not set");

    let adsb_address = format!("{}:{}", adsb_host, adsb_port);

    // Connect to the ADS-B stream (using environment variables)
    let stream = TcpStream::connect(&adsb_address)?;
    let reader = BufReader::new(stream);

    // Initialize MQTT client using environment variables
    let mut mqtt_client = mqtt_publisher::init_mqtt_client();

    for line in reader.lines() {
        match line {
            Ok(data) => {
                // Parse the ADS-B message
                if let Some(parsed_message) = adsb_parser::parse_adsb_message(&data) {
                    // Pass mqtt_client as mutable
                    mqtt_publisher::publish_to_mqtt(&mut mqtt_client, &parsed_message);
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

    Ok(())
}
