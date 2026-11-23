//! `#[derive(Wire)]` for `fdb-native-wire`. Use it through the wire crate's re-export,
//! `fdb_native_wire::Wire` (the `derive` feature, on by default); see [`macro@Wire`].
//!
//! The two crates depend on each other: `fdb-native-wire` depends (optionally) on this
//! crate, and this crate dev-depends on `fdb-native-wire` for its tests and doc examples.
//! Cargo allows that cycle, but it constrains publishing: this crate must be published
//! first, and a dev-dependency that carries a version must already exist on the
//! registry. Make the dev-dependency path-only for that publish (Cargo strips path-only
//! dev-dependencies from the published manifest), then publish `fdb-native-wire` at the
//! matching version.

mod attrs;
mod expand;

use proc_macro::TokenStream;

/// Derives `Wire`, `Encode` and `Decode<'de>` (and `FileIdentified` with
/// `#[fdb(file_identifier = N)]`) for a FoundationDB wire type: a table, a union
/// (`std::variant`) or an inline struct (`std::tuple`).
///
/// # Field order is `serializer` argument order
///
/// **The order of the Rust fields is the order of the arguments of the C++
/// `serializer(ar, ...)` call, and it decides the vtable slot of every field.** It is not
/// the order of the C++ member declarations, which often differs. Reordering, adding or
/// removing a Rust field changes the wire format; a field in the wrong place still
/// compiles and still round-trips through Rust, and only a C++ peer notices. Copy the
/// field list from the `serializer` call, not from the C++ struct body:
///
/// ```
/// use fdb_native_wire::Wire;
///
/// // struct OrderDemo {
/// //     uint8_t a; uint64_t b; std::string c; uint16_t d;
/// //     template <class Ar> void serialize(Ar& ar) { serializer(ar, d, c, a, b); }
/// // };
/// #[derive(Wire, Debug, PartialEq)]
/// #[fdb(file_identifier = 0x000fdb10)]
/// struct OrderDemo<'a> {
///     d: u16,      // slot 0
///     c: &'a [u8], // slot 1
///     a: u8,       // slot 2
///     b: u64,      // slot 3
/// }
///
/// let value = OrderDemo { d: 1, c: b"c", a: 2, b: 3 };
/// let bytes = fdb_native_wire::encode(&value)?;
/// assert_eq!(fdb_native_wire::decode::<OrderDemo>(&bytes)?, value);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Kinds of type
///
/// | Rust | wire | C++ |
/// | --- | --- | --- |
/// | struct (named, tuple or unit) | table: one `field()` per field, in order | a type with `serialize(Ar&)` |
/// | struct with `#[fdb(struct)]` | inline struct: members laid out in order, each aligned to its size | `std::tuple` (or a `struct_like_traits` type such as `Tag`) |
/// | enum whose variants each hold one value | union: variant *k* is wire tag *k* + 1 | `std::variant` |
///
/// A **table** with no serialized fields (a unit struct, `struct S {}`, or only `skip`
/// fields) is an *empty table*, like `Void`: C++ never follows the offset to it, so it
/// decodes with `Reader::empty_table`. Any other table decodes with `Reader::table`.
///
/// A **union**'s variants are its alternatives in C++ declaration order; each variant
/// holds exactly one value, its payload. Encoding variant *k* writes tag *k* + 1;
/// decoding maps tag *k* + 1 to variant *k* and any other tag (0, or one past the last
/// alternative) to `absent()`, the first alternative's absent value, as C++ does. A
/// union alternative cannot be a union: wrap it in `EnsureTable`. (`Option<T>` and
/// `ErrorOr<T>` are unions already implemented by the wire crate.)
///
/// An **inline struct** (`#[fdb(struct)]`) is stored inline in its table, vector or
/// union, `Shape::of_struct` of its members. Every serialized member must be inline (a
/// scalar, `Uid`, ...) and not itself a struct; anything else fails to compile, at the
/// type's definition for a non-generic type and where it is used otherwise.
///
/// ```
/// use fdb_native_wire::{Shape, Wire};
///
/// // std::tuple<int16_t, bool, int64_t>
/// #[derive(Wire, Clone, Copy, Debug, PartialEq)]
/// #[fdb(struct)]
/// struct Triple {
///     a: i16, // offset 0
///     b: bool, // offset 2
///     c: i64, // offset 8
/// }
/// assert_eq!(<Triple as Wire>::SHAPE, Shape::Inline { size: 16, align: 8 });
///
/// // std::variant<int32_t, double, Triple>
/// #[derive(Wire, Debug, PartialEq)]
/// enum Value {
///     I32(i32),   // tag 1
///     F64(f64),   // tag 2
///     Triple(Triple), // tag 3
/// }
///
/// #[derive(Wire, Debug, PartialEq)]
/// #[fdb(file_identifier = 0x000fdb0c)]
/// struct Holder {
///     before: u8,
///     value: Value, // two slots: the tag, then the offset
///     after: u16,
/// }
///
/// let holder = Holder { before: 1, value: Value::Triple(Triple { a: -1, b: true, c: 7 }), after: 2 };
/// let bytes = fdb_native_wire::encode(&holder)?;
/// assert_eq!(fdb_native_wire::decode::<Holder>(&bytes)?, holder);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// ```compile_fail,E0080
/// use fdb_native_wire::Wire;
///
/// // A struct member must be inline: a string is not.
/// #[derive(Wire)]
/// #[fdb(struct)]
/// struct NotInline {
///     a: u32,
///     b: Vec<u8>,
/// }
/// ```
///
/// ```compile_fail,E0080
/// use fdb_native_wire::Wire;
///
/// // FDB does not support nested structs.
/// #[derive(Wire)]
/// #[fdb(struct)]
/// struct Inner(u8, u32);
///
/// #[derive(Wire)]
/// #[fdb(struct)]
/// struct Outer(Inner, u8);
/// ```
///
/// ```compile_fail,E0080
/// use fdb_native_wire::Wire;
///
/// // ErrorOr<Optional<T>> does not compile in C++ either: use EnsureTable<Option<u8>>.
/// #[derive(Wire)]
/// enum V {
///     A(u8),
///     B(Option<u8>),
/// }
/// ```
///
/// # Attributes
///
/// On the type:
///
/// | attribute | meaning |
/// | --- | --- |
/// | `#[fdb(file_identifier = N)]` | tables only (a C++ `std::variant` or `std::tuple` has none). Implements `FileIdentified` with `FileIdentifier::new(N)`. `N` is an integer literal or a constant expression below 2^24 (larger fails to compile). Wrappers compose it automatically: `ErrorOr<EnsureTable<T>>`, `Option<T>`, `Vec<T>`, ... |
/// | `#[fdb(struct)]` | an inline struct (`std::tuple`) instead of a table. Structs only. |
/// | `#[fdb(crate = "path")]` | the path of the wire crate in the generated code (default `::fdb_native_wire`); for code that renames the crate or re-exports it. |
///
/// ```compile_fail,E0080
/// use fdb_native_wire::Wire;
///
/// // Already composed (ErrorOr<Inner>): give the plain identifier, the wrappers compose it.
/// const ERROR_OR_INNER: u32 = 0x020f_db0a;
///
/// #[derive(Wire)]
/// #[fdb(file_identifier = ERROR_OR_INNER)]
/// struct Inner {
///     a: u32,
/// }
/// ```
///
/// On a field of a table (or struct, for `skip`):
///
/// | attribute | slots | meaning |
/// | --- | --- | --- |
/// | (none) | 1, or 2 for a union or a vector of unions | a `serializer` argument |
/// | `#[fdb(skip)]` | 0 | not on the wire at all. Encoding ignores it; decoding and `absent()` set it to `Default::default()`. Does not count as a `serializer` argument. |
/// | `#[fdb(arena)]` | 0 | the `Arena` argument of `serializer(ar, ..., arena)`. Nothing is written or read and the field is `Default::default()`, but it counts as an argument: a table whose only argument is an `Arena` is *not* an empty table, because C++ follows the offset to it and reads its vtable. It must be the last serialized field (C++ requires the `Arena` last), at most one per table. Any `Default` type works; `()` is enough. |
/// | `#[fdb(skip_slots = N)]` | `N` | `N` slots this type does not model (a field of another protocol version, a type with no Rust equivalent yet). Encoding leaves them absent (vtable entry 0), which every FDB reader decodes as the default value; decoding steps over them. The field is `Default::default()`; a union takes 2 slots. |
///
/// ```
/// use fdb_native_wire::{Uid, Wire};
///
/// // struct GetThing { Key key; Version version; Optional<UID> debugID;
/// //     ReplyPromise<...> reply; SomethingNew extra; Arena arena;
/// //     serializer(ar, key, version, debugID, reply, extra, arena); };
/// #[derive(Wire, Debug, PartialEq)]
/// #[fdb(file_identifier = 0x000fdb0f)]
/// struct GetThing<'a> {
///     key: &'a [u8],
///     version: i64,
///     debug_id: Option<Uid>, // two slots
///     reply_token: ReplyToken,
///     #[fdb(skip_slots = 1)]
///     extra: (), // not modelled: written absent, not read
///     #[fdb(arena)]
///     arena: (),
///     #[fdb(skip)]
///     received_at: Option<std::time::Instant>, // local bookkeeping, not on the wire
/// }
///
/// // ReplyPromise<T> is a table holding the reply endpoint's token.
/// #[derive(Wire, Debug, PartialEq)]
/// struct ReplyToken {
///     token: Uid,
/// }
///
/// let request = GetThing {
///     key: b"k",
///     version: 7,
///     debug_id: None,
///     reply_token: ReplyToken { token: Uid::new(1, 2) },
///     extra: (),
///     arena: (),
///     received_at: None,
/// };
/// let bytes = fdb_native_wire::encode(&request)?;
/// assert_eq!(fdb_native_wire::decode::<GetThing>(&bytes)?, request);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Protocol versions
///
/// There are no version-gating attributes. A message whose layout differs between
/// protocol versions (FDB 8.0 removed fields from the middle of some requests) gets one
/// Rust type per version, in per-version schema sets (`schema::v7_4::GetValueRequest`,
/// `schema::v8_0::GetValueRequest`), each deriving its exact layout; the client logic
/// works on version-neutral domain types and converts at the edge. `skip_slots` covers a
/// slot a version's type does not model. `crates/fdb-native-wire/tests/release_messages.rs`
/// does this for `GetValueRequest` and `GetValueReply` and checks the result against the
/// messages FDB 7.4 and 8.0 write.
///
/// # Generics and lifetimes
///
/// Fields can borrow from the message (`&'a [u8]`, `Vec<&'a [u8]>`, ...): the `Decode`
/// impl is `impl<'__fdb_de: 'a, 'a> Decode<'__fdb_de> for T<'a>`, so a decoded value
/// borrows from the buffer it was decoded from. Any number of lifetime, type and const
/// parameters are allowed. Each field type that mentions a type parameter gets the bound
/// the impl needs (`Vec<T>: Encode`, `Vec<T>: Decode<'__fdb_de>`, `Default` for `skip`,
/// `arena` and `skip_slots` fields, `Wire` for the members of a struct or union), so a
/// parameter only used in a skipped field (`PhantomData<T>`) needs no bound.
///
/// # What is generated
///
/// For
///
/// ```
/// # use fdb_native_wire::{Uid, Wire};
/// #[derive(Wire)]
/// #[fdb(file_identifier = 1039114)]
/// struct Inner<'a> {
///     a: u32,
///     b: &'a [u8],
///     #[fdb(arena)]
///     arena: (),
/// }
/// ```
///
/// the derive generates (paths shortened):
///
/// ```text
/// impl<'a> Wire for Inner<'a> {
///     const SHAPE: Shape = Shape::Table;
/// }
///
/// impl<'a> Encode for Inner<'a> {
///     fn encode_object(&self, __fdb_w: &mut Writer) -> Result<ObjectRef, EncodeError> {
///         let mut __fdb_t = __fdb_w.table();
///         __fdb_t.field::<u32>(&self.a)?;
///         __fdb_t.field::<&'a [u8]>(&self.b)?;
///         __fdb_t.finish()
///     }
/// }
///
/// impl<'__fdb_de: 'a, 'a> Decode<'__fdb_de> for Inner<'a> {
///     fn absent() -> Self {
///         Self {
///             a: <u32 as Decode<'__fdb_de>>::absent(),
///             b: <&'a [u8] as Decode<'__fdb_de>>::absent(),
///             arena: <() as Default>::default(),
///         }
///     }
///
///     fn decode_object(__fdb_r: &Reader<'__fdb_de>, __fdb_at: usize) -> Result<Self, DecodeError> {
///         let mut __fdb_t = __fdb_r.table(__fdb_at)?;
///         let __fdb_f0: u32 = __fdb_t.field::<u32>()?;
///         let __fdb_f1: &'a [u8] = __fdb_t.field::<&'a [u8]>()?;
///         Ok(Self { a: __fdb_f0, b: __fdb_f1, arena: <() as Default>::default() })
///     }
/// }
///
/// impl<'a> FileIdentified for Inner<'a> {
///     const FILE_IDENTIFIER: FileIdentifier = FileIdentifier::new(1039114);
/// }
/// const _: FileIdentifier = FileIdentifier::new(1039114);
/// ```
///
/// Types the derive does not cover (blob codecs, types whose file identifier composes
/// another type's like `ReplyPromise<T>`) implement the traits by hand; the wire crate's
/// docs describe how.
#[proc_macro_derive(Wire, attributes(fdb))]
pub fn derive_wire(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    expand::expand(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
