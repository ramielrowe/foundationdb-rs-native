//! FlowTransport library types: [`ReplyPromise`], [`RequestStream`] and the [`Endpoint`]
//! it carries, and [`VectorRef`].
//!
//! These are the types FDB's request and interface structs are made of. Their wire forms
//! come from `fdbrpc/fdbrpc.h` (`serializable_traits<ReplyPromise<T>>`,
//! `serializable_traits<RequestStream<T, P>>`), `fdbrpc/FlowTransport.h` (`Endpoint`),
//! `flow/NetworkAddress.h` and `flow/IPAddress.h`; they are identical on 7.4 and 8.0, and
//! `tools/golden-vectors` checks them against both branches' serializers.

use core::fmt;
use core::hash::{Hash, Hasher};
use core::marker::PhantomData;
use core::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use core::ops::{Deref, DerefMut};

use crate::{
    Decode, DecodeError, Encode, EncodeError, FileIdentified, FileIdentifier, ObjectRef, Reader,
    Shape, Uid, UnionRef, Wire, Wrapper, Writer,
};

// ---------------------------------------------------------------------------------------
// ReplyPromise

/// FDB's `ReplyPromise<T>`: where a request's reply goes, as it travels in the request.
///
/// On the wire it is a table with one slot, the reply endpoint's token (a [`Uid`]); the
/// peer sends the reply to the sender's address with that token. `T` is the reply type
/// (`ReplyPromise<GetValueReply>`): it is not on the wire, but it composes into the file
/// identifier, which is `T`'s with [`Wrapper::ReplyPromise`] (code 1), so
/// `ReplyPromise<Inner>` with `Inner` = `0x000fdb0a` is `0x010fdb0a`.
///
/// ```
/// use fdb_native_wire::{FileIdentified, ReplyPromise, Uid, Void};
///
/// let reply = ReplyPromise::<Void>::new(Uid::new(1, 2));
/// assert_eq!(reply.token(), Uid::new(1, 2));
/// assert_eq!(<ReplyPromise<Void>>::FILE_IDENTIFIER.value(), 0x011e_ad4a);
///
/// let bytes = fdb_native_wire::encode(&reply)?;
/// assert_eq!(fdb_native_wire::decode::<ReplyPromise<Void>>(&bytes)?, reply);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct ReplyPromise<T> {
    token: Uid,
    reply: PhantomData<fn() -> T>,
}

impl<T> ReplyPromise<T> {
    /// A reply promise whose reply endpoint has `token`.
    pub const fn new(token: Uid) -> Self {
        ReplyPromise {
            token,
            reply: PhantomData,
        }
    }

    /// The reply endpoint's token.
    pub const fn token(&self) -> Uid {
        self.token
    }
}

impl<T> Clone for ReplyPromise<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for ReplyPromise<T> {}

impl<T> Default for ReplyPromise<T> {
    fn default() -> Self {
        ReplyPromise::new(Uid::default())
    }
}

impl<T> PartialEq for ReplyPromise<T> {
    fn eq(&self, other: &Self) -> bool {
        self.token == other.token
    }
}

impl<T> Eq for ReplyPromise<T> {}

impl<T> Hash for ReplyPromise<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.token.hash(state);
    }
}

impl<T> fmt::Debug for ReplyPromise<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyPromise")
            .field("token", &self.token)
            .finish()
    }
}

impl<T> Wire for ReplyPromise<T> {
    const SHAPE: Shape = Shape::Table;
}

impl<T> Encode for ReplyPromise<T> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let mut t = w.table();
        t.field(&self.token)?;
        t.finish()
    }
}

impl<'de, T> Decode<'de> for ReplyPromise<T> {
    fn absent() -> Self {
        ReplyPromise::default()
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        Ok(ReplyPromise::new(t.field()?))
    }
}

impl<T: FileIdentified> FileIdentified for ReplyPromise<T> {
    const FILE_IDENTIFIER: FileIdentifier = T::FILE_IDENTIFIER.compose(Wrapper::ReplyPromise);
}

// ---------------------------------------------------------------------------------------
// Endpoint and its parts

/// FDB's `IPAddress`: a table with one slot holding `std::variant<uint32_t,
/// std::array<uint8_t, 16>>` (tag 1 IPv4, tag 2 IPv6).
///
/// The IPv4 `u32` is the address as a number (10.0.0.1 is `0x0a000001`); the IPv6 bytes
/// are in network order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IpAddress {
    /// An IPv4 address, as a number.
    V4(u32),
    /// An IPv6 address, as 16 bytes in network order.
    V6([u8; 16]),
}

impl Default for IpAddress {
    /// `IPAddress()`: IPv4 `0.0.0.0`.
    fn default() -> Self {
        IpAddress::V4(0)
    }
}

impl From<IpAddr> for IpAddress {
    fn from(ip: IpAddr) -> Self {
        match ip {
            IpAddr::V4(ip) => IpAddress::V4(ip.into()),
            IpAddr::V6(ip) => IpAddress::V6(ip.octets()),
        }
    }
}

impl From<IpAddress> for IpAddr {
    fn from(ip: IpAddress) -> Self {
        match ip {
            IpAddress::V4(ip) => IpAddr::V4(Ipv4Addr::from(ip)),
            IpAddress::V6(ip) => IpAddr::V6(Ipv6Addr::from(ip)),
        }
    }
}

/// The union in an `IPAddress` table (the `std::variant` member `addr`).
struct IpUnion(IpAddress);

impl Wire for IpUnion {
    const SHAPE: Shape = Shape::Union;
}

impl Encode for IpUnion {
    fn encode_union(&self, w: &mut Writer) -> Result<UnionRef, EncodeError> {
        match &self.0 {
            IpAddress::V4(ip) => w.union_value(1, ip),
            IpAddress::V6(ip) => w.union_value(2, ip),
        }
    }
}

impl<'de> Decode<'de> for IpUnion {
    fn absent() -> Self {
        IpUnion(IpAddress::default())
    }

    fn decode_union(r: &Reader<'de>, tag: u8, at: usize) -> Result<Self, DecodeError> {
        match tag {
            1 => r.alternative(at).map(|ip| IpUnion(IpAddress::V4(ip))),
            2 => r.alternative(at).map(|ip| IpUnion(IpAddress::V6(ip))),
            _ => Ok(Self::absent()),
        }
    }
}

impl Wire for IpAddress {
    const SHAPE: Shape = Shape::Table;
}

impl Encode for IpAddress {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let mut t = w.table();
        t.field(&IpUnion(*self))?;
        t.finish()
    }
}

impl<'de> Decode<'de> for IpAddress {
    fn absent() -> Self {
        IpAddress::default()
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        Ok(t.field::<IpUnion>()?.0)
    }
}

/// FDB's `NetworkAddress` (`serializer(ar, ip, port, flags, fromHostname)`): a table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct NetworkAddress {
    /// The IP address.
    pub ip: IpAddress,
    /// The port.
    pub port: u16,
    /// [`FLAG_PRIVATE`](NetworkAddress::FLAG_PRIVATE) and [`FLAG_TLS`](NetworkAddress::FLAG_TLS).
    pub flags: u16,
    /// Whether the address was resolved from a hostname.
    pub from_hostname: bool,
}

impl NetworkAddress {
    /// `FLAG_PRIVATE`: the address is not public (no peer outside the process's network
    /// can reach it).
    pub const FLAG_PRIVATE: u16 = 1;
    /// `FLAG_TLS`: the address speaks TLS.
    pub const FLAG_TLS: u16 = 2;

    /// A public or private, TLS or plain address for `ip:port`.
    pub const fn new(ip: IpAddress, port: u16, is_public: bool, is_tls: bool) -> Self {
        NetworkAddress {
            ip,
            port,
            flags: (if is_public { 0 } else { Self::FLAG_PRIVATE })
                | (if is_tls { Self::FLAG_TLS } else { 0 }),
            from_hostname: false,
        }
    }

    /// `isPublic()`.
    pub const fn is_public(&self) -> bool {
        self.flags & Self::FLAG_PRIVATE == 0
    }

    /// `isTLS()`.
    pub const fn is_tls(&self) -> bool {
        self.flags & Self::FLAG_TLS != 0
    }

    /// The socket address.
    pub fn socket_addr(&self) -> std::net::SocketAddr {
        std::net::SocketAddr::new(self.ip.into(), self.port)
    }
}

impl Default for NetworkAddress {
    /// `NetworkAddress()`: `0.0.0.0:0`, private, plain.
    fn default() -> Self {
        NetworkAddress {
            ip: IpAddress::default(),
            port: 0,
            flags: Self::FLAG_PRIVATE,
            from_hostname: false,
        }
    }
}

impl Wire for NetworkAddress {
    const SHAPE: Shape = Shape::Table;
}

impl Encode for NetworkAddress {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let mut t = w.table();
        t.field(&self.ip)?;
        t.field(&self.port)?;
        t.field(&self.flags)?;
        t.field(&self.from_hostname)?;
        t.finish()
    }
}

impl<'de> Decode<'de> for NetworkAddress {
    /// `NetworkAddress()`: C++ default-constructs a missing table, and the constructor
    /// sets `FLAG_PRIVATE`. (A present table with a missing `flags` slot reads 0.)
    fn absent() -> Self {
        NetworkAddress::default()
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        Ok(NetworkAddress {
            ip: t.field()?,
            port: t.field()?,
            flags: t.field()?,
            from_hostname: t.field()?,
        })
    }
}

/// FDB's `NetworkAddressList` (`serializer(ar, address, secondaryAddress)`): a table.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct NetworkAddressList {
    /// The primary address.
    pub address: NetworkAddress,
    /// The secondary address (a process listening on both plain and TLS has two).
    pub secondary_address: Option<NetworkAddress>,
}

impl Wire for NetworkAddressList {
    const SHAPE: Shape = Shape::Table;
}

impl Encode for NetworkAddressList {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let mut t = w.table();
        t.field(&self.address)?;
        t.field(&self.secondary_address)?;
        t.finish()
    }
}

impl<'de> Decode<'de> for NetworkAddressList {
    fn absent() -> Self {
        NetworkAddressList::default()
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        Ok(NetworkAddressList {
            address: t.field()?,
            secondary_address: t.field()?,
        })
    }
}

/// FDB's `Endpoint` (`serializer(ar, addresses, token)`): a table. Where a service
/// listens (an address list) and the token that selects it there.
///
/// Decoding does not apply `Endpoint::choosePrimaryAddress()`, which C++ calls after
/// reading: it swaps the two addresses when only the secondary matches the local TLS
/// setting. That depends on the local network, so the transport layer applies it; this is
/// the endpoint as it is on the wire.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Endpoint {
    /// Where the service listens.
    pub addresses: NetworkAddressList,
    /// The token that selects the service.
    pub token: Uid,
}

impl Endpoint {
    /// An endpoint at `addresses` with `token`.
    pub const fn new(addresses: NetworkAddressList, token: Uid) -> Self {
        Endpoint { addresses, token }
    }

    /// `Endpoint::wellKnownToken`: `UID(-1, id)`.
    pub const fn well_known_token(id: u64) -> Uid {
        Uid::new(u64::MAX, id)
    }

    /// `Endpoint::getAdjustedEndpoint(index)`: the `index`th endpoint of an interface whose
    /// base endpoint this is. Interfaces serialize only their first endpoint; the others
    /// are this endpoint with the index added to the low half of the token's second word
    /// (as a `u32`, wrapping) and shifted into the high half of its first.
    ///
    /// ```
    /// use fdb_native_wire::{Endpoint, Uid};
    ///
    /// let base = Endpoint { token: Uid::new(10, 0xaaaa_bbbb_ffff_ffff), ..Endpoint::default() };
    /// let adjusted = base.adjusted(2);
    /// assert_eq!(adjusted.token, Uid::new(10 + (2 << 32), 0xaaaa_bbbb_0000_0001));
    /// ```
    pub const fn adjusted(&self, index: u32) -> Endpoint {
        let new_index = (self.token.second as u32).wrapping_add(index);
        Endpoint {
            addresses: self.addresses,
            token: Uid::new(
                self.token.first.wrapping_add((index as u64) << 32),
                (self.token.second & 0xffff_ffff_0000_0000) | new_index as u64,
            ),
        }
    }
}

impl Wire for Endpoint {
    const SHAPE: Shape = Shape::Table;
}

impl Encode for Endpoint {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let mut t = w.table();
        t.field(&self.addresses)?;
        t.field(&self.token)?;
        t.finish()
    }
}

impl<'de> Decode<'de> for Endpoint {
    fn absent() -> Self {
        Endpoint::default()
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        Ok(Endpoint {
            addresses: t.field()?,
            token: t.field()?,
        })
    }
}

// ---------------------------------------------------------------------------------------
// RequestStream

/// FDB's `RequestStream<T>` (and `PublicRequestStream<T>`, the same on the wire): a
/// service's endpoint, as it appears in an interface (`StorageServerInterface::getValue`,
/// ...).
///
/// On the wire it is a table with one slot holding an [`Endpoint`]. `T` is the request
/// type; it is not on the wire. FDB gives `RequestStream` no file identifier (it is never
/// a message's root), so this type is not [`FileIdentified`]. Interfaces serialize only
/// their first stream; the others are [`Endpoint::adjusted`].
///
/// `ReplyPromiseStream<T>` (streaming replies) is not implemented.
///
/// ```
/// use fdb_native_wire::{Endpoint, RequestStream};
///
/// struct GetValueRequest;
/// let stream = RequestStream::<GetValueRequest>::new(Endpoint::default());
/// assert_eq!(stream.endpoint(), &Endpoint::default());
/// ```
pub struct RequestStream<T> {
    endpoint: Endpoint,
    request: PhantomData<fn() -> T>,
}

impl<T> RequestStream<T> {
    /// A stream at `endpoint`.
    pub const fn new(endpoint: Endpoint) -> Self {
        RequestStream {
            endpoint,
            request: PhantomData,
        }
    }

    /// The stream's endpoint.
    pub const fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
}

impl<T> Clone for RequestStream<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for RequestStream<T> {}

impl<T> Default for RequestStream<T> {
    fn default() -> Self {
        RequestStream::new(Endpoint::default())
    }
}

impl<T> PartialEq for RequestStream<T> {
    fn eq(&self, other: &Self) -> bool {
        self.endpoint == other.endpoint
    }
}

impl<T> Eq for RequestStream<T> {}

impl<T> Hash for RequestStream<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.endpoint.hash(state);
    }
}

impl<T> fmt::Debug for RequestStream<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestStream")
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl<T> Wire for RequestStream<T> {
    const SHAPE: Shape = Shape::Table;
}

impl<T> Encode for RequestStream<T> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let mut t = w.table();
        t.field(&self.endpoint)?;
        t.finish()
    }
}

impl<'de, T> Decode<'de> for RequestStream<T> {
    fn absent() -> Self {
        RequestStream::default()
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        Ok(RequestStream::new(t.field()?))
    }
}

// ---------------------------------------------------------------------------------------
// VectorRef

/// FDB's `VectorRef<T>` (and `Standalone<VectorRef<T>>`) where its file identifier
/// matters: a vector on the wire, exactly like `std::vector<T>` and so like [`Vec<T>`],
/// but composing `T`'s file identifier with [`Wrapper::VectorRef`] (code 3) instead of
/// [`Wrapper::Vector`] (code 5).
///
/// Use `Vec<T>` for a `VectorRef` or `std::vector` *field*: a field's wire form carries no
/// identifier, and the encoded bytes are identical (`fid_vectorref` and `fid_std_vector`
/// differ only in the identifier). Use this type where the vector is a message's root or
/// the payload of a reply (`ReplyPromise<Standalone<VectorRef<T>>>`, whose reply travels
/// as `ErrorOr<EnsureTable<VectorRef<T>>>`): the identifier is part of the message.
///
/// It dereferences to the [`Vec<T>`] it wraps. It is the `VecSerStrategy::FlatBuffers`
/// form; `VectorRef<KeyValueRef, VecSerStrategy::String>` is a different wire form (one
/// blob) and a different type.
///
/// ```
/// use fdb_native_wire::{FdbError, FileIdentified, VectorRef};
///
/// let list = VectorRef(vec![FdbError::new(1), FdbError::new(2)]);
/// assert_eq!(list.len(), 2);
/// assert_eq!(
///     <VectorRef<FdbError>>::FILE_IDENTIFIER.value() >> 24,
///     3, // Wrapper::VectorRef
/// );
/// let bytes = fdb_native_wire::encode(&list)?;
/// assert_eq!(fdb_native_wire::decode::<VectorRef<FdbError>>(&bytes)?, list);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VectorRef<T>(pub Vec<T>);

impl<T> Deref for VectorRef<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Vec<T> {
        &self.0
    }
}

impl<T> DerefMut for VectorRef<T> {
    fn deref_mut(&mut self) -> &mut Vec<T> {
        &mut self.0
    }
}

impl<T> From<Vec<T>> for VectorRef<T> {
    fn from(v: Vec<T>) -> Self {
        VectorRef(v)
    }
}

impl<T> From<VectorRef<T>> for Vec<T> {
    fn from(v: VectorRef<T>) -> Self {
        v.0
    }
}

impl<T> FromIterator<T> for VectorRef<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        VectorRef(iter.into_iter().collect())
    }
}

impl<T> IntoIterator for VectorRef<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a VectorRef<T> {
    type Item = &'a T;
    type IntoIter = core::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<T: Wire> Wire for VectorRef<T> {
    const SHAPE: Shape = <Vec<T> as Wire>::SHAPE;
}

impl<T: Encode> Encode for VectorRef<T> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        self.0.encode_object(w)
    }

    fn encode_union_vector(&self, w: &mut Writer) -> Result<(ObjectRef, ObjectRef), EncodeError> {
        self.0.encode_union_vector(w)
    }
}

impl<'de, T: Decode<'de>> Decode<'de> for VectorRef<T> {
    fn absent() -> Self {
        VectorRef(Vec::absent())
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        Vec::decode_object(r, at).map(VectorRef)
    }

    fn decode_union_vector(
        r: &Reader<'de>,
        tags_at: usize,
        offsets_at: usize,
    ) -> Result<Self, DecodeError> {
        Vec::decode_union_vector(r, tags_at, offsets_at).map(VectorRef)
    }
}

impl<T: FileIdentified> FileIdentified for VectorRef<T> {
    const FILE_IDENTIFIER: FileIdentifier = T::FILE_IDENTIFIER.compose(Wrapper::VectorRef);
}

/// `StringRef` (and so `Key`, `Value` and every other `Standalone<StringRef>`) has file
/// identifier 13300811 (`flow/include/flow/Arena.h`). `&[u8]` is the Rust `StringRef`, so
/// `VectorRef<&[u8]>` composes to `0x03caf44b`. `std::string` has none.
impl FileIdentified for [u8] {
    const FILE_IDENTIFIER: FileIdentifier = FileIdentifier::new(13_300_811);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Void, decode, encode};

    #[test]
    fn composed_identifiers() {
        // Void is 2010442 = 0x1ead4a.
        assert_eq!(<ReplyPromise<Void>>::FILE_IDENTIFIER.value(), 0x011e_ad4a);
        assert_eq!(<VectorRef<Void>>::FILE_IDENTIFIER.value(), 0x031e_ad4a);
        assert_eq!(<Vec<Void>>::FILE_IDENTIFIER.value(), 0x051e_ad4a);
        // ReplyPromise<Optional<Void>>: depth 2, (1 << 28) | 0x04_1ead4a.
        assert_eq!(
            <ReplyPromise<Option<Void>>>::FILE_IDENTIFIER.value(),
            0x141e_ad4a
        );
    }

    #[test]
    fn string_ref_identifier() {
        assert_eq!(<VectorRef<&[u8]>>::FILE_IDENTIFIER.value(), 0x03ca_f44b);
        assert_eq!(<Vec<&[u8]>>::FILE_IDENTIFIER.value(), 0x05ca_f44b);
    }

    #[test]
    fn round_trips() {
        let reply = ReplyPromise::<Void>::new(Uid::new(u64::MAX, 7));
        let bytes = encode(&reply).expect("encode");
        assert_eq!(&bytes[4..8], &0x011e_ad4au32.to_le_bytes());
        assert_eq!(decode::<ReplyPromise<Void>>(&bytes).expect("decode"), reply);

        let list = VectorRef(vec![Void, Void]);
        let bytes = encode(&list).expect("encode");
        assert_eq!(&bytes[4..8], &0x031e_ad4au32.to_le_bytes());
        assert_eq!(decode::<VectorRef<Void>>(&bytes).expect("decode"), list);
    }

    #[test]
    fn adjusted_endpoint_wraps() {
        let base = Endpoint::new(
            NetworkAddressList::default(),
            Uid::new(u64::MAX, 0x1234_5678_ffff_fffe),
        );
        let e = base.adjusted(3);
        assert_eq!(e.token.first, u64::MAX.wrapping_add(3 << 32));
        assert_eq!(e.token.second, 0x1234_5678_0000_0001);
        assert_eq!(base.adjusted(0), base);
    }

    #[test]
    fn ip_conversions() {
        let v4 = IpAddress::V4(0x0a00_0001);
        assert_eq!(IpAddr::from(v4), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(IpAddress::from(IpAddr::from(v4)), v4);
        let v6 = IpAddress::V6(Ipv6Addr::LOCALHOST.octets());
        assert_eq!(IpAddress::from(IpAddr::from(v6)), v6);
    }
}
