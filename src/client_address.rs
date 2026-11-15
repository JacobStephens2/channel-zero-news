//! The client address `POST /api/rooms` counts live rooms against.
//!
//! The service binds loopback behind the local Apache, which appends the
//! address it accepted each request from to `X-Forwarded-For`
//! (`ProxyAddHeaders` defaults on). The TCP peer decides what to trust:
//!
//! * A loopback peer is that Apache. The rightmost `X-Forwarded-For` entry is
//!   the client; entries left of it came from the client itself. With no
//!   header, or a rightmost entry that is not an IP address, the peer is used.
//! * Any other peer is the client: its own address, forwarded headers ignored.
//! * With no known peer (the router served without peer addresses, as the
//!   integration tests serve it), every request shares [`ClientAddress::NO_PEER`].
//!
//! IPv4-mapped IPv6 is keyed as IPv4. Other IPv6 is keyed by its /64, because
//! one host can draw fresh addresses from its /64 at will (temporary SLAAC
//! addresses, RFC 8981).

use std::convert::Infallible;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};

use axum::async_trait;
use axum::extract::{ConnectInfo, FromRequestParts};
use axum::http::{request::Parts, HeaderMap};

/// The key one client's live rooms are counted under: an IPv4 address, an IPv6
/// /64, or [`Self::NO_PEER`]. As a handler argument it resolves from the
/// request and never rejects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientAddress(Option<IpAddr>);

impl ClientAddress {
    /// The one key every request without a peer address shares.
    pub const NO_PEER: Self = Self(None);

    /// Resolve the client from the TCP `peer` (`None` when the router is
    /// served without peer addresses) and the request `headers`.
    pub fn resolve(peer: Option<IpAddr>, headers: &HeaderMap) -> Self {
        let Some(peer) = peer.map(|peer| peer.to_canonical()) else {
            return Self::NO_PEER;
        };
        if peer.is_loopback() {
            if let Some(forwarded) = rightmost_forwarded_for(headers) {
                return Self::from(forwarded);
            }
        }
        Self::from(peer)
    }
}

/// Keys IPv4-mapped IPv6 as IPv4, and other IPv6 by its /64.
impl From<IpAddr> for ClientAddress {
    fn from(ip: IpAddr) -> Self {
        Self(Some(match ip.to_canonical() {
            IpAddr::V6(v6) => IpAddr::V6(Ipv6Addr::from(u128::from(v6) & SLASH_64_MASK)),
            v4 => v4,
        }))
    }
}

/// Keeps the network half of an IPv6 address.
const SLASH_64_MASK: u128 = u128::MAX << 64;

/// The peer comes from [`ConnectInfo`], which only [`crate::app::serve`]
/// provides; without it the request resolves to [`ClientAddress::NO_PEER`].
#[async_trait]
impl<S: Send + Sync> FromRequestParts<S> for ClientAddress {
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let peer = Option::<ConnectInfo<SocketAddr>>::from_request_parts(parts, state)
            .await?
            .map(|ConnectInfo(addr)| addr.ip());
        Ok(Self::resolve(peer, &parts.headers))
    }
}

/// The last entry of the last `X-Forwarded-For` line, the one the local Apache
/// appended, when it is an IP address.
fn rightmost_forwarded_for(headers: &HeaderMap) -> Option<IpAddr> {
    let line = headers.get_all("x-forwarded-for").iter().next_back()?;
    let entry = line.to_str().ok()?.rsplit(',').next()?;
    entry.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue};

    fn forwarded_for(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", HeaderValue::from_str(value).unwrap());
        headers
    }

    #[test]
    fn with_no_known_peer_every_request_shares_one_key_whatever_it_forwards() {
        let keys = [
            ClientAddress::resolve(None, &forwarded_for("203.0.113.7")),
            ClientAddress::resolve(None, &forwarded_for("198.51.100.9")),
            ClientAddress::resolve(None, &HeaderMap::new()),
        ];
        assert_eq!(keys, [ClientAddress::NO_PEER; 3]);
    }

    fn ip(text: &str) -> IpAddr {
        text.parse().unwrap()
    }

    #[test]
    fn a_peer_other_than_loopback_is_keyed_by_its_own_address_and_its_forwarded_header_is_ignored()
    {
        let key = ClientAddress::resolve(Some(ip("198.51.100.20")), &forwarded_for("203.0.113.7"));
        assert_eq!(key, ClientAddress::from(ip("198.51.100.20")));
        assert_ne!(key, ClientAddress::from(ip("203.0.113.7")));
    }

    #[test]
    fn from_the_local_apache_the_key_is_the_rightmost_forwarded_entry_not_a_client_supplied_one() {
        let mut separate_lines = forwarded_for("198.51.100.9");
        separate_lines.append("x-forwarded-for", HeaderValue::from_static("203.0.113.7"));
        for headers in [forwarded_for("198.51.100.9, 203.0.113.7"), separate_lines] {
            assert_eq!(
                ClientAddress::resolve(Some(ip("127.0.0.1")), &headers),
                ClientAddress::from(ip("203.0.113.7")),
                "{headers:?}"
            );
        }
    }

    #[test]
    fn from_the_local_apache_a_missing_or_unparseable_rightmost_entry_keys_the_peer() {
        let local = ip("127.0.0.1");
        let mut cases = vec![HeaderMap::new()];
        for value in [
            "not-an-address",
            "203.0.113.7, unknown",
            "203.0.113.7,",
            "203.0.113.7:4711",
            "",
        ] {
            cases.push(forwarded_for(value));
        }
        let mut opaque = HeaderMap::new();
        opaque.insert("x-forwarded-for", HeaderValue::from_bytes(b"\xff").unwrap());
        cases.push(opaque);
        for headers in cases {
            assert_eq!(
                ClientAddress::resolve(Some(local), &headers),
                ClientAddress::from(local),
                "{headers:?}"
            );
        }
    }

    #[test]
    fn ipv6_clients_are_keyed_by_their_slash_64() {
        let forwarded =
            |entry: &str| ClientAddress::resolve(Some(ip("127.0.0.1")), &forwarded_for(entry));
        let peer = |address: &str| ClientAddress::resolve(Some(ip(address)), &HeaderMap::new());
        assert_eq!(
            forwarded("2001:db8:0:1::5"),
            forwarded("2001:db8:0:1:ffff:ffff:ffff:ffff")
        );
        assert_eq!(peer("2001:db8:0:1::abcd"), forwarded("2001:db8:0:1::5"));
        assert_ne!(forwarded("2001:db8:0:1::5"), forwarded("2001:db8:0:2::5"));
    }

    #[test]
    fn ipv4_mapped_addresses_are_keyed_as_ipv4() {
        let client = ClientAddress::from(ip("203.0.113.7"));
        let cases = [
            ("127.0.0.1", "::ffff:203.0.113.7"),
            ("::ffff:127.0.0.1", "203.0.113.7"),
            ("::ffff:203.0.113.7", "198.51.100.9"),
        ];
        for (peer, entry) in cases {
            assert_eq!(
                ClientAddress::resolve(Some(ip(peer)), &forwarded_for(entry)),
                client,
                "peer {peer}, forwarded {entry}"
            );
        }
    }
}
