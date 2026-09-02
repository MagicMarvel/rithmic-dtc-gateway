use std::sync::OnceLock;

static SYNTHETIC_MAC: OnceLock<String> = OnceLock::new();

/// Returns one random locally administered unicast MAC for this process.
///
/// Every Rithmic Plant and reconnect in the bridge uses the same synthetic
/// identity, while no physical adapter address is ever inspected or sent.
pub fn synthetic_mac() -> String {
    SYNTHETIC_MAC.get_or_init(generate).clone()
}

fn generate() -> String {
    let mut octets: [u8; 6] = rand::random();
    octets[0] = (octets[0] | 0x02) & 0xfe;
    octets
        .iter()
        .map(|octet| format!("{octet:02X}"))
        .collect::<Vec<_>>()
        .join(":")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_is_stable_local_unicast_and_not_a_hardware_lookup() {
        let first = synthetic_mac();
        let second = synthetic_mac();
        assert_eq!(first, second);
        let octet = u8::from_str_radix(&first[..2], 16).unwrap();
        assert_eq!(octet & 0x01, 0);
        assert_eq!(octet & 0x02, 0x02);
    }
}
