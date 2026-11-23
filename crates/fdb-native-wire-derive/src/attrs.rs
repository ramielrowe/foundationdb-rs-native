//! Parsing of `#[fdb(...)]` attributes.

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::{Attribute, Error, Expr, Lit, LitInt, LitStr, Path, Result};

/// Attributes on the type.
pub(crate) struct ContainerAttrs {
    /// `crate = "path"`: the path of the wire crate.
    pub(crate) krate: Option<Path>,
    /// `file_identifier = expr`, with the span of `file_identifier`.
    pub(crate) file_identifier: Option<(Span, Expr)>,
    /// `struct`: an inline struct rather than a table.
    pub(crate) inline_struct: Option<Span>,
}

/// What a field (or variant field) is on the wire.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum FieldKind {
    /// A `serializer` argument: one `field()` call (one or two slots).
    Field,
    /// `skip`: not serialized at all; decoded as `Default::default()`.
    Skip,
    /// `arena`: an `Arena` `serializer` argument. No slot and no bytes, but it counts as
    /// an argument, so a table with only an arena is not an empty table.
    Arena,
    /// `skip_slots = N`: `N` slots this type does not model, written absent and not read.
    SkipSlots(usize),
}

/// Attributes on a field.
pub(crate) struct FieldAttrs {
    pub(crate) kind: FieldKind,
    /// The span of the attribute that set `kind`, for error messages.
    pub(crate) span: Option<Span>,
}

fn is_fdb(attr: &Attribute) -> bool {
    attr.path().is_ident("fdb")
}

fn duplicate(span: Span, name: &str) -> Error {
    Error::new(span, format!("duplicate `{name}` attribute"))
}

pub(crate) fn container(attrs: &[Attribute]) -> Result<ContainerAttrs> {
    let mut out = ContainerAttrs {
        krate: None,
        file_identifier: None,
        inline_struct: None,
    };
    for attr in attrs.iter().filter(|a| is_fdb(a)) {
        attr.parse_nested_meta(|meta| {
            let span = meta.path.span();
            if meta.path.is_ident("crate") {
                if out.krate.is_some() {
                    return Err(duplicate(span, "crate"));
                }
                let s: LitStr = meta.value()?.parse()?;
                out.krate = Some(s.parse()?);
            } else if meta.path.is_ident("file_identifier") {
                if out.file_identifier.is_some() {
                    return Err(duplicate(span, "file_identifier"));
                }
                let value = meta.value()?;
                // syn parses a subset of expressions without its "full" feature; do not
                // pass its message (which suggests enabling that feature) to the user.
                let expr: Expr = value.parse().map_err(|e| {
                    Error::new(
                        e.span(),
                        "expected an integer literal or simple constant expression",
                    )
                })?;
                check_file_identifier(&expr)?;
                out.file_identifier = Some((span, expr));
            } else if meta.path.is_ident("struct") {
                if out.inline_struct.is_some() {
                    return Err(duplicate(span, "struct"));
                }
                out.inline_struct = Some(span);
            } else if meta.path.is_ident("skip")
                || meta.path.is_ident("arena")
                || meta.path.is_ident("skip_slots")
            {
                return Err(meta.error("this is a field attribute; put it on a field"));
            } else {
                return Err(meta.error(
                    "unknown fdb attribute; expected `file_identifier = N`, `struct` or \
                     `crate = \"path\"`",
                ));
            }
            Ok(())
        })?;
    }
    Ok(out)
}

/// Rejects a literal file identifier that is not a plain (below 2^24) identifier here,
/// with the literal's span; other expressions are checked by a generated constant.
fn check_file_identifier(expr: &Expr) -> Result<()> {
    let Expr::Lit(lit) = expr else {
        return Ok(());
    };
    if let Lit::Int(int) = &lit.lit {
        let value = int.base10_parse::<u64>()?;
        if value >= 1 << 24 {
            return Err(Error::new(
                int.span(),
                "a file identifier must be below 2^24 (0x1000000); wrapper codes in the top \
                 byte are composed automatically",
            ));
        }
    }
    Ok(())
}

pub(crate) fn field(attrs: &[Attribute]) -> Result<FieldAttrs> {
    let mut out = FieldAttrs {
        kind: FieldKind::Field,
        span: None,
    };
    for attr in attrs.iter().filter(|a| is_fdb(a)) {
        attr.parse_nested_meta(|meta| {
            let span = meta.path.span();
            let kind = if meta.path.is_ident("skip") {
                FieldKind::Skip
            } else if meta.path.is_ident("arena") {
                FieldKind::Arena
            } else if meta.path.is_ident("skip_slots") {
                let n: LitInt = meta.value()?.parse()?;
                let slots = n.base10_parse::<usize>()?;
                if slots == 0 {
                    return Err(Error::new(n.span(), "`skip_slots` must be at least 1"));
                }
                FieldKind::SkipSlots(slots)
            } else if meta.path.is_ident("file_identifier")
                || meta.path.is_ident("struct")
                || meta.path.is_ident("crate")
            {
                return Err(meta.error("this is a container attribute; put it on the type"));
            } else {
                return Err(meta.error(
                    "unknown fdb field attribute; expected `skip`, `arena` or `skip_slots = N`",
                ));
            };
            if out.span.is_some() {
                return Err(Error::new(
                    span,
                    "a field takes at most one of `skip`, `arena` and `skip_slots`",
                ));
            }
            out.kind = kind;
            out.span = Some(span);
            Ok(())
        })?;
    }
    Ok(out)
}

/// Rejects `#[fdb]` attributes where none are supported (enum variants).
pub(crate) fn none(attrs: &[Attribute], what: &str) -> Result<()> {
    match attrs.iter().find(|a| is_fdb(a)) {
        Some(attr) => Err(Error::new_spanned(
            attr,
            format!("fdb attributes are not supported on {what}"),
        )),
        None => Ok(()),
    }
}
