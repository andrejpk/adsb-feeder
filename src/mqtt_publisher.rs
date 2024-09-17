use crate::adsb_parser::AdsbMessage;
use colored::*;
use indicatif::{ProgressBar, ProgressStyle}; // For progress bar
use rumqttc::{Client, MqttOptions, QoS};
use serde::Serialize;
use std::env;
use std::thread;
use std::time::Duration; // For colored output

#[derive(Serialize)]
struct MqttMessage<'a> {
    hex_ident: &'a str,
    transmission_type: u8,
    altitude: Option<u32>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    ground_speed: Option<f64>,
    track: Option<f64>,
}

// Initialize MQTT client and connect
pub fn init_mqtt_client() -> Client {
    // Load MQTT host, port, username, and password from environment variables
    let mqtt_host = env::var("MQTT_HOST").expect("MQTT_HOST not set");
    let mqtt_port: u16 = env::var("MQTT_PORT")
        .expect("MQTT_PORT not set")
        .parse()
        .expect("MQTT_PORT should be a valid u16");

    let mqtt_username = env::var("MQTT_USERNAME").expect("MQTT_USERNAME not set");
    let mqtt_password = env::var("MQTT_PASSWORD").expect("MQTT_PASSWORD not set");
    let _verbose = false;

    let mut mqttoptions = MqttOptions::new("adsb_publisher", mqtt_host, mqtt_port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    // Set MQTT authentication credentials
    mqttoptions.set_credentials(mqtt_username, mqtt_password);

    let (client, mut connection) = Client::new(mqttoptions, 10);
    thread::spawn(move || {
        for notification in connection.iter() {
            if _verbose {
                // make notification green if Ok, red if Err
                let color = match notification {
                    Ok(_) => "green",
                    Err(_) => "red",
                };
                println!(
                    " {}: {}",
                    "MQTT Notification".white(),
                    format!("{:?}", notification).color(color)
                );
            }
        }
    });

    client
}

// Function to publish the ADS-B message to an MQTT broker
pub fn publish_to_mqtt(client: &mut Client, message: &AdsbMessage, progress: &ProgressBar) {
    let topic = format!("adsb/{}/{}", message.hex_ident, message.transmission_type);
    let _verbose = false;

    let mqtt_message = MqttMessage {
        hex_ident: &message.hex_ident,
        transmission_type: message.transmission_type,
        altitude: message.altitude,
        latitude: message.latitude,
        longitude: message.longitude,
        ground_speed: message.ground_speed,
        track: message.track,
    };

    let payload = serde_json::to_string(&mqtt_message).unwrap();

    // Try to publish the message to the MQTT broker
    let result = client.publish(topic.clone(), QoS::AtLeastOnce, false, payload);
    match result {
        Ok(_) => {
            // Successful publish: Display with a green checkmark
            if _verbose {
                println!(
                    "{} {} {}",
                    "✔".green(),
                    "Published to topic:".green(),
                    topic.bright_yellow()
                );
            }
            progress.set_message(format!("Published to topic: {}", topic.bright_yellow()));
            progress.inc(1);
        }
        Err(err) => {
            // Failed publish: Display with a red cross
            // if _verbose {
            println!(
                "{} {}: {} {}",
                "✘".red(),
                "Failed to publish to topic".red(),
                topic.bright_yellow(),
                err.to_string().red()
            );
            // }
            progress.set_message(format!("Failed to topic:    {}", topic.bright_yellow()));
            progress.inc(1);
        }
    }
}
