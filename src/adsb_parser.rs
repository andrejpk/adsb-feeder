// adsb_parser.rs
#[derive(Debug)]
pub struct AdsbMessage {
    pub message_type: String,
    pub transmission_type: u8,
    pub session_id: String,
    pub aircraft_id: String,
    pub hex_ident: String,
    pub flight_id: String,
    pub date_gen: String,
    pub time_gen: String,
    pub altitude: Option<u32>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub ground_speed: Option<f64>,
    pub track: Option<f64>,
}

// Function to parse a single ADS-B message string
pub fn parse_adsb_message(line: &str) -> Option<AdsbMessage> {
    let parts: Vec<&str> = line.split(',').collect();

    if parts.len() < 22 || parts[0] != "MSG" {
        // Return None if the message doesn't have the right structure or isn't an MSG message
        return None;
    }

    Some(AdsbMessage {
        message_type: parts[0].to_string(),
        transmission_type: parts[1].parse::<u8>().unwrap_or(0),
        session_id: parts[2].to_string(),
        aircraft_id: parts[3].to_string(),
        hex_ident: parts[4].to_string(),
        flight_id: parts[5].to_string(),
        date_gen: parts[6].to_string(),
        time_gen: parts[7].to_string(),
        altitude: parts[11].parse::<u32>().ok(),
        latitude: parts[14].parse::<f64>().ok(),
        longitude: parts[15].parse::<f64>().ok(),
        ground_speed: parts[12].parse::<f64>().ok(),
        track: parts[13].parse::<f64>().ok(),
    })
}
