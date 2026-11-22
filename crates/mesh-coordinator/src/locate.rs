//! Working out where a node is from the address the coordinator reaches it
//! at, instead of believing what the node claims.
//!
//! Two tables the operator downloads and points the coordinator at:
//!
//! - DB-IP's free "IP to City Lite" CSV maps address ranges to a country and
//!   a latitude and longitude. It is free with attribution: "IP Geolocation
//!   by DB-IP" (<https://db-ip.com>).
//! - iptoasn.com's `ip2asn-combined.tsv` maps address ranges to the AS
//!   number they are announced from, which is the node's network.
//!
//! An address can be made to look like it is somewhere else, with a VPN or
//! a tunnel, so the location is checked against the speed of light: light
//! in fibre covers about 200 km per millisecond, so a round trip of `t`
//! milliseconds puts the node at most `t × 100` km from the coordinator. A
//! location further away than that is not believed.

use std::net::IpAddr;
use std::path::Path;
use std::time::Duration;

/// Kilometres light in fibre covers per millisecond of round trip: 200 km
/// each way per millisecond, half of the round trip each way.
const KM_PER_ROUND_TRIP_MS: f64 = 100.0;

/// The earth's mean radius, for distances between two places.
const EARTH_RADIUS_KM: f64 = 6371.0;

/// Where an address range is, from the city table.
#[derive(Clone, Debug, PartialEq)]
pub struct Place {
    /// The two-letter country code, such as `DE`.
    pub country: String,
    pub latitude: f64,
    pub longitude: f64,
}

/// Address ranges, each with a value, sorted by their start.
pub struct RangeTable<T> {
    ranges: Vec<(u128, u128, T)>,
}

impl<T> RangeTable<T> {
    fn new(mut ranges: Vec<(u128, u128, T)>) -> Self {
        ranges.sort_by_key(|(start, _, _)| *start);
        RangeTable { ranges }
    }

    /// The value of the range `address` falls in.
    pub fn find(&self, address: IpAddr) -> Option<&T> {
        let address = as_number(address);
        // The last range starting at or before the address is the only one
        // that can hold it; the tables have no overlapping ranges.
        let after = self
            .ranges
            .partition_point(|(start, _, _)| *start <= address);
        let (_, end, value) = self.ranges.get(after.checked_sub(1)?)?;
        (address <= *end).then_some(value)
    }
}

/// What the coordinator looks node addresses up in.
pub struct Locator {
    places: Option<RangeTable<Place>>,
    networks: Option<RangeTable<u32>>,
    /// Where the coordinator is, as latitude and longitude.
    here: Option<(f64, f64)>,
}

/// Where a node is, as far as the coordinator can tell.
#[derive(Debug, Default, PartialEq)]
pub struct Located {
    /// The AS the address is announced from, such as `AS3320`.
    pub network: Option<String>,
    /// The country, when the round trip does not rule it out.
    pub country: Option<String>,
}

impl Locator {
    /// A locator over the tables at `city_csv` and `asn_tsv`, either of which
    /// may be missing, for a coordinator at `here`.
    pub fn load(
        city_csv: Option<&Path>,
        asn_tsv: Option<&Path>,
        here: Option<(f64, f64)>,
    ) -> std::io::Result<Self> {
        let places = match city_csv {
            Some(path) => Some(parse_city_csv(&std::fs::read_to_string(path)?)),
            None => None,
        };
        let networks = match asn_tsv {
            Some(path) => Some(parse_asn_tsv(&std::fs::read_to_string(path)?)),
            None => None,
        };
        Ok(Locator {
            places,
            networks,
            here,
        })
    }

    /// A locator that knows nothing, for a coordinator without tables.
    pub fn empty() -> Self {
        Locator {
            places: None,
            networks: None,
            here: None,
        }
    }

    /// Where the node at `address` is, given that it answered in
    /// `round_trip`.
    pub fn locate(&self, address: IpAddr, round_trip: Duration) -> Located {
        let network = self
            .networks
            .as_ref()
            .and_then(|table| table.find(address))
            .map(|number| format!("AS{number}"));
        let country = self
            .places
            .as_ref()
            .and_then(|table| table.find(address))
            .filter(|place| self.is_reachable_in(place, round_trip))
            .map(|place| place.country.clone());
        Located { network, country }
    }

    /// Whether a node at `place` could have answered in `round_trip`. Without
    /// knowing where the coordinator is, nothing can be ruled out.
    fn is_reachable_in(&self, place: &Place, round_trip: Duration) -> bool {
        let Some(here) = self.here else {
            return true;
        };
        let reach = round_trip.as_secs_f64() * 1000.0 * KM_PER_ROUND_TRIP_MS;
        distance_km(here, (place.latitude, place.longitude)) <= reach
    }
}

/// The distance between two places on the earth's surface, by the haversine
/// formula.
pub fn distance_km(from: (f64, f64), to: (f64, f64)) -> f64 {
    let (from_latitude, to_latitude) = (from.0.to_radians(), to.0.to_radians());
    let latitude_change = to_latitude - from_latitude;
    let longitude_change = (to.1 - from.1).to_radians();
    let half_chord = (latitude_change / 2.0).sin().powi(2)
        + from_latitude.cos() * to_latitude.cos() * (longitude_change / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_KM * half_chord.sqrt().asin()
}

/// An address as one number, with IPv4 mapped into IPv6, so both families
/// share one table.
fn as_number(address: IpAddr) -> u128 {
    match address {
        IpAddr::V4(v4) => u128::from(v4.to_ipv6_mapped()),
        IpAddr::V6(v6) => u128::from(v6),
    }
}

/// The ranges of DB-IP's city lite CSV: start, end, continent, country,
/// region, city, latitude, longitude. Lines that don't parse are skipped.
fn parse_city_csv(text: &str) -> RangeTable<Place> {
    let ranges = text
        .lines()
        .filter_map(|line| {
            let fields = csv_fields(line);
            let start: IpAddr = fields.first()?.parse().ok()?;
            let end: IpAddr = fields.get(1)?.parse().ok()?;
            let place = Place {
                country: fields.get(3)?.clone(),
                latitude: fields.get(6)?.parse().ok()?,
                longitude: fields.get(7)?.parse().ok()?,
            };
            Some((as_number(start), as_number(end), place))
        })
        .collect();
    RangeTable::new(ranges)
}

/// The ranges of iptoasn's TSV: start, end, AS number, country, name. Ranges
/// announced by no AS have number 0 and are skipped.
fn parse_asn_tsv(text: &str) -> RangeTable<u32> {
    let ranges = text
        .lines()
        .filter_map(|line| {
            let mut fields = line.split('\t');
            let start: IpAddr = fields.next()?.parse().ok()?;
            let end: IpAddr = fields.next()?.parse().ok()?;
            let number: u32 = fields.next()?.parse().ok()?;
            (number != 0).then_some((as_number(start), as_number(end), number))
        })
        .collect();
    RangeTable::new(ranges)
}

/// The fields of one CSV line, with double-quoted fields unquoted. DB-IP
/// quotes fields holding commas, such as some city names.
fn csv_fields(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut characters = line.chars().peekable();
    while let Some(character) = characters.next() {
        match character {
            '"' if quoted && characters.peek() == Some(&'"') => {
                field.push('"');
                characters.next();
            }
            '"' => quoted = !quoted,
            ',' if !quoted => fields.push(std::mem::take(&mut field)),
            _ => field.push(character),
        }
    }
    fields.push(field);
    fields
}

#[cfg(test)]
mod tests {
    use super::*;

    const CITIES: &str = "\
1.0.0.0,1.0.0.255,OC,AU,Queensland,\"South Brisbane, Qld\",-27.4748,153.017
5.9.0.0,5.9.255.255,EU,DE,Saxony,Falkenstein,50.4779,12.3713
2a01:4f8::,2a01:4f8:ffff:ffff:ffff:ffff:ffff:ffff,EU,DE,Saxony,Falkenstein,50.4779,12.3713
not,a,line
";

    const NETWORKS: &str = "\
5.9.0.0\t5.9.255.255\t24940\tDE\tHETZNER-AS
6.0.0.0\t6.0.0.255\t0\tNone\tNot routed
";

    fn locator(here: Option<(f64, f64)>) -> Locator {
        Locator {
            places: Some(parse_city_csv(CITIES)),
            networks: Some(parse_asn_tsv(NETWORKS)),
            here,
        }
    }

    const FRANKFURT: (f64, f64) = (50.1109, 8.6821);

    #[test]
    fn an_address_is_found_in_its_range_in_either_family() {
        let table = parse_city_csv(CITIES);

        assert_eq!(
            table.find("1.0.0.9".parse().unwrap()).unwrap().country,
            "AU"
        );
        assert_eq!(
            table.find("5.9.3.4".parse().unwrap()).unwrap().country,
            "DE"
        );
        assert_eq!(
            table.find("2a01:4f8::1".parse().unwrap()).unwrap().country,
            "DE"
        );
        assert_eq!(table.find("5.10.0.0".parse().unwrap()), None);
        assert_eq!(table.find("0.0.0.1".parse().unwrap()), None);
    }

    #[test]
    fn quoted_fields_keep_their_commas() {
        assert_eq!(
            csv_fields("a,\"b, c\",\"say \"\"hi\"\"\""),
            vec!["a", "b, c", "say \"hi\""]
        );
    }

    #[test]
    fn a_node_near_enough_for_its_round_trip_is_located() {
        let located =
            locator(Some(FRANKFURT)).locate("5.9.3.4".parse().unwrap(), Duration::from_millis(8));

        assert_eq!(
            located,
            Located {
                network: Some("AS24940".into()),
                country: Some("DE".into()),
            }
        );
    }

    #[test]
    fn a_place_too_far_for_the_round_trip_is_not_believed() {
        // Brisbane is about 16,000 km from Frankfurt; 10 ms reach 1,000 km.
        let located =
            locator(Some(FRANKFURT)).locate("1.0.0.7".parse().unwrap(), Duration::from_millis(10));

        assert_eq!(located.country, None);
    }

    #[test]
    fn unannounced_or_unknown_addresses_have_no_network() {
        let locator = locator(None);

        assert_eq!(
            locator
                .locate("6.0.0.1".parse().unwrap(), Duration::ZERO)
                .network,
            None
        );
        assert_eq!(
            Locator::empty().locate("5.9.3.4".parse().unwrap(), Duration::ZERO),
            Located::default()
        );
    }

    #[test]
    fn distances_match_known_ones() {
        let berlin = (52.52, 13.405);
        let distance = distance_km(FRANKFURT, berlin);

        assert!((420.0..430.0).contains(&distance), "{distance}");
    }
}
