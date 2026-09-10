//! SSRF 兩層 deny 的 IP／hostname 分類。
//!
//! 只做「這個位址屬於哪一層」。`NetworkRule`／`Source` 白名單是 Phase 3 connector-sdk。
//! 規則來源：`docs/security/CONNECTOR_SECURITY.md` §3a。

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// 分類結果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpClass {
    /// Cloud metadata 等，永遠不可覆寫。
    HardDeny,
    /// RFC1918／loopback／link-local／CGNAT／ULA，可由 Source NetworkRule 覆寫。
    SoftDeny,
    /// 公開位址。
    Public,
}

impl IpClass {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HardDeny => "hard_deny",
            Self::SoftDeny => "soft_deny",
            Self::Public => "public",
        }
    }
}

/// 與 `IpClass` 同義，給文件用語。
pub type DenyTier = IpClass;

/// 分類一個 IP。hostname 請先解析再呼叫；硬 deny 的特殊 hostname 用 [`classify_host`]。
#[must_use]
pub fn classify_ip(ip: IpAddr) -> IpClass {
    match ip {
        IpAddr::V4(v4) => classify_v4(v4),
        IpAddr::V6(v6) => classify_v6(v6),
    }
}

/// 分類 hostname（大小寫不敏感）。未知 hostname 回 `None`，呼叫端應解析成 IP 再 [`classify_ip`]。
#[must_use]
pub fn classify_host(host: &str) -> Option<IpClass> {
    let host = host.trim().trim_end_matches('.').to_ascii_lowercase();
    if HARD_DENY_HOSTS.contains(&host.as_str()) {
        return Some(IpClass::HardDeny);
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Some(classify_ip(ip));
    }
    None
}

const HARD_DENY_HOSTS: &[&str] = &[
    "metadata.google.internal",
    "metadata.google.com",
    "metadata.goog",
];

fn classify_v4(ip: Ipv4Addr) -> IpClass {
    let octets = ip.octets();
    // AWS / OpenStack / GCP / DigitalOcean IMDS
    if ip == Ipv4Addr::new(169, 254, 169, 254) {
        return IpClass::HardDeny;
    }
    // Alibaba Cloud IMDS
    if ip == Ipv4Addr::new(100, 100, 100, 200) {
        return IpClass::HardDeny;
    }
    // Azure IMDS
    if ip == Ipv4Addr::new(169, 254, 169, 253) || ip == Ipv4Addr::new(168, 63, 129, 16) {
        return IpClass::HardDeny;
    }

    if ip.is_loopback() || ip.is_unspecified() || ip.is_broadcast() {
        return IpClass::SoftDeny;
    }
    if ip.is_link_local() {
        return IpClass::SoftDeny;
    }
    // RFC1918
    if octets[0] == 10
        || (octets[0] == 172 && (16..32).contains(&octets[1]))
        || (octets[0] == 192 && octets[1] == 168)
    {
        return IpClass::SoftDeny;
    }
    // CGNAT / shared address space RFC6598
    if octets[0] == 100 && (64..128).contains(&octets[1]) {
        return IpClass::SoftDeny;
    }
    // IETF protocol assignments / TEST-NET / multicast / reserved
    if octets[0] == 0
        || octets[0] == 127
        || (octets[0] == 192 && octets[1] == 0 && octets[2] == 2)
        || (octets[0] == 198 && octets[1] == 51 && octets[2] == 100)
        || (octets[0] == 203 && octets[1] == 0 && octets[2] == 113)
        || octets[0] >= 224
    {
        return IpClass::SoftDeny;
    }
    IpClass::Public
}

fn classify_v6(ip: Ipv6Addr) -> IpClass {
    if let Some(v4) = ip.to_ipv4_mapped() {
        return classify_v4(v4);
    }
    // AWS IMDS IPv6
    if ip == Ipv6Addr::new(0xfd00, 0xec2, 0, 0, 0, 0, 0, 0x254) {
        return IpClass::HardDeny;
    }
    if ip.is_loopback() || ip.is_unspecified() {
        return IpClass::SoftDeny;
    }
    // link-local fe80::/10
    let segs = ip.segments();
    if (segs[0] & 0xffc0) == 0xfe80 {
        return IpClass::SoftDeny;
    }
    // unique local fc00::/7
    if (segs[0] & 0xfe00) == 0xfc00 {
        return IpClass::SoftDeny;
    }
    // multicast
    if ip.is_multicast() {
        return IpClass::SoftDeny;
    }
    // IPv4-compatible deprecated
    if segs[0] == 0 && segs[1] == 0 && segs[2] == 0 && segs[3] == 0 && segs[4] == 0 && segs[5] == 0
    {
        if let Some(v4) = ip.to_ipv4() {
            return classify_v4(v4);
        }
    }
    IpClass::Public
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v4(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(a, b, c, d))
    }

    #[test]
    fn hard_deny_metadata() {
        assert_eq!(classify_ip(v4(169, 254, 169, 254)), IpClass::HardDeny);
        assert_eq!(classify_ip(v4(169, 254, 169, 253)), IpClass::HardDeny);
        assert_eq!(classify_ip(v4(168, 63, 129, 16)), IpClass::HardDeny);
        assert_eq!(classify_ip(v4(100, 100, 100, 200)), IpClass::HardDeny);
        let aws6: IpAddr = "fd00:ec2::254".parse().unwrap();
        assert_eq!(classify_ip(aws6), IpClass::HardDeny);
        assert_eq!(
            classify_host("metadata.google.internal"),
            Some(IpClass::HardDeny)
        );
        assert_eq!(
            classify_host("METADATA.GOOGLE.INTERNAL."),
            Some(IpClass::HardDeny)
        );
    }

    #[test]
    fn soft_deny_private() {
        assert_eq!(classify_ip(v4(10, 0, 0, 1)), IpClass::SoftDeny);
        assert_eq!(classify_ip(v4(172, 16, 0, 1)), IpClass::SoftDeny);
        assert_eq!(classify_ip(v4(192, 168, 1, 1)), IpClass::SoftDeny);
        assert_eq!(classify_ip(v4(127, 0, 0, 1)), IpClass::SoftDeny);
        assert_eq!(classify_ip(v4(169, 254, 1, 1)), IpClass::SoftDeny);
        assert_eq!(classify_ip(v4(100, 64, 0, 1)), IpClass::SoftDeny);
        let ula: IpAddr = "fd12:3456::1".parse().unwrap();
        assert_eq!(classify_ip(ula), IpClass::SoftDeny);
        let lo6: IpAddr = "::1".parse().unwrap();
        assert_eq!(classify_ip(lo6), IpClass::SoftDeny);
        let mapped: IpAddr = "::ffff:10.1.2.3".parse().unwrap();
        assert_eq!(classify_ip(mapped), IpClass::SoftDeny);
    }

    #[test]
    fn public_examples() {
        assert_eq!(classify_ip(v4(8, 8, 8, 8)), IpClass::Public);
        assert_eq!(classify_ip(v4(1, 1, 1, 1)), IpClass::Public);
        let v6: IpAddr = "2001:4860:4860::8888".parse().unwrap();
        assert_eq!(classify_ip(v6), IpClass::Public);
        assert_eq!(classify_host("example.com"), None);
    }

    #[test]
    fn hard_deny_is_not_just_link_local() {
        // 169.254.0.0/16 是 soft-deny，但 .169.254 是 hard-deny。
        assert_eq!(classify_ip(v4(169, 254, 0, 1)), IpClass::SoftDeny);
        assert_eq!(classify_ip(v4(169, 254, 169, 254)), IpClass::HardDeny);
    }
}
