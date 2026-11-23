//! Code generation.

use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, format_ident, quote, quote_spanned};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};
use syn::{
    Data, DataEnum, DeriveInput, Error, Expr, Fields, GenericParam, Generics, Ident, LifetimeParam,
    Member, Path, Result, Type, WhereClause, WherePredicate, parse_quote,
};

use crate::attrs::{self, FieldKind};

/// The lifetime of the buffer in the generated `Decode<'de>` impl.
const DE: &str = "'__fdb_de";

struct Cx<'a> {
    krate: Path,
    name: &'a Ident,
    generics: &'a Generics,
}

struct Field<'a> {
    member: Member,
    ty: &'a Type,
    kind: FieldKind,
    /// The attribute's span, or the field's.
    span: Span,
}

pub(crate) fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let attrs = attrs::container(&input.attrs)?;
    let krate = attrs
        .krate
        .clone()
        .unwrap_or_else(|| parse_quote!(::fdb_native_wire));
    let cx = Cx {
        krate,
        name: &input.ident,
        generics: &input.generics,
    };
    let body = match &input.data {
        Data::Struct(s) => {
            let fields = fields(&s.fields)?;
            if attrs.inline_struct.is_some() {
                expand_struct(&cx, &fields)?
            } else {
                expand_table(&cx, &fields)?
            }
        }
        Data::Enum(e) => {
            if let Some(span) = attrs.inline_struct {
                return Err(Error::new(
                    span,
                    "`#[fdb(struct)]` only applies to structs; an enum derives a union",
                ));
            }
            expand_union(&cx, e)?
        }
        Data::Union(u) => {
            return Err(Error::new_spanned(
                u.union_token,
                "Rust unions cannot derive Wire; a wire union (std::variant) is an enum whose \
                 variants each hold one value, like `enum V { A(i32), B(Inner) }`",
            ));
        }
    };
    let file_identifier = match &attrs.file_identifier {
        None => None,
        // C++ std::variant and std::tuple have no file identifier.
        Some((span, _)) if matches!(input.data, Data::Enum(_)) => {
            return Err(Error::new(
                *span,
                "a union (std::variant) has no file identifier; put `file_identifier` on a \
                 table, or wrap the union in one",
            ));
        }
        Some((span, _)) if attrs.inline_struct.is_some() => {
            return Err(Error::new(
                *span,
                "an `#[fdb(struct)]` (std::tuple) has no file identifier; put \
                 `file_identifier` on a table",
            ));
        }
        Some((_, expr)) => Some(expand_file_identifier(&cx, expr)),
    };
    Ok(quote! {
        #body
        #file_identifier
    })
}

fn fields(fields: &Fields) -> Result<Vec<Field<'_>>> {
    let mut out = Vec::new();
    let mut errors: Option<Error> = None;
    for (field, member) in fields.iter().zip(fields.members()) {
        match attrs::field(&field.attrs) {
            Ok(a) => out.push(Field {
                member,
                ty: &field.ty,
                kind: a.kind,
                span: a.span.unwrap_or_else(|| field.span()),
            }),
            Err(e) => combine(&mut errors, e),
        }
    }
    match errors {
        Some(e) => Err(e),
        None => Ok(out),
    }
}

fn combine(errors: &mut Option<Error>, e: Error) {
    match errors {
        Some(errors) => errors.combine(e),
        None => *errors = Some(e),
    }
}

// ---------------------------------------------------------------------------------------
// Tables.

fn expand_table(cx: &Cx<'_>, fields: &[Field<'_>]) -> Result<TokenStream> {
    // C++ requires an Arena to be the last serializer argument (static_assert in
    // ObjectSerializerTraits.h); since declaration order is argument order, so do we.
    let mut arena: Option<&Field<'_>> = None;
    for f in fields {
        match f.kind {
            FieldKind::Arena => {
                if arena.is_some() {
                    return Err(Error::new(
                        f.span,
                        "a table takes at most one `#[fdb(arena)]` field (C++ allows one \
                         Arena serializer argument, the last)",
                    ));
                }
                arena = Some(f);
            }
            FieldKind::Field | FieldKind::SkipSlots(_) => {
                if let Some(a) = arena {
                    return Err(Error::new(
                        a.span,
                        "an `#[fdb(arena)]` field must come after every serialized field: \
                         C++ requires the Arena to be the last serializer argument, and \
                         field order is argument order",
                    ));
                }
            }
            FieldKind::Skip => {}
        }
    }

    let krate = &cx.krate;
    let de = de_lifetime();
    // `serializer(ar)` with no arguments: C++ never follows the offset to the table. An
    // Arena counts as an argument (and then the table is followed and its vtable read).
    let empty = fields.iter().all(|f| f.kind == FieldKind::Skip);

    let mut encode_steps = Vec::new();
    let mut decode_steps = Vec::new();
    let mut inits = Vec::new();
    for (i, f) in fields.iter().enumerate() {
        let (member, ty) = (&f.member, f.ty);
        let var = format_ident!("__fdb_f{}", i);
        match f.kind {
            FieldKind::Field => {
                encode_steps.push(quote! {
                    __fdb_t.field::<#ty>(&self.#member)?;
                });
                decode_steps.push(quote! {
                    let #var: #ty = __fdb_t.field::<#ty>()?;
                });
                inits.push(quote!(#member: #var));
            }
            FieldKind::SkipSlots(n) => {
                encode_steps.push(quote!(__fdb_t.skip(#n);));
                decode_steps.push(quote!(__fdb_t.skip(#n);));
                inits.push(default_init(member, ty));
            }
            FieldKind::Skip | FieldKind::Arena => inits.push(default_init(member, ty)),
        }
    }

    let encode_body = if encode_steps.is_empty() {
        quote!(__fdb_w.table().finish())
    } else {
        quote! {
            let mut __fdb_t = __fdb_w.table();
            #(#encode_steps)*
            __fdb_t.finish()
        }
    };
    let decode_body = if empty {
        quote!(__fdb_r.empty_table(__fdb_at)?;)
    } else if decode_steps.is_empty() {
        // Only an Arena (and skipped fields): the table is followed and checked.
        quote!(__fdb_r.table(__fdb_at)?;)
    } else {
        quote! {
            let mut __fdb_t = __fdb_r.table(__fdb_at)?;
            #(#decode_steps)*
        }
    };

    let absent = absent_inits(cx, fields);
    let wire = impl_wire(cx, &[], quote!(#krate::Shape::Table), None);
    let (encode_generics, decode_generics) = codec_bounds(cx, fields);
    let name = cx.name;
    let (impl_g, ty_g, _) = cx.generics.split_for_impl();
    let decode_g = decode_generics_params(cx.generics);
    let (decode_impl_g, _, _) = decode_g.split_for_impl();
    Ok(quote! {
        #wire

        #[automatically_derived]
        impl #impl_g #krate::Encode for #name #ty_g #encode_generics {
            fn encode_object(
                &self,
                __fdb_w: &mut #krate::Writer,
            ) -> ::core::result::Result<#krate::ObjectRef, #krate::EncodeError> {
                #encode_body
            }
        }

        #[automatically_derived]
        impl #decode_impl_g #krate::Decode<#de> for #name #ty_g #decode_generics {
            fn absent() -> Self {
                Self { #(#absent),* }
            }

            fn decode_object(
                __fdb_r: &#krate::Reader<#de>,
                __fdb_at: usize,
            ) -> ::core::result::Result<Self, #krate::DecodeError> {
                #decode_body
                ::core::result::Result::Ok(Self { #(#inits),* })
            }
        }
    })
}

// ---------------------------------------------------------------------------------------
// Inline structs.

fn expand_struct(cx: &Cx<'_>, fields: &[Field<'_>]) -> Result<TokenStream> {
    if let Some(f) = fields
        .iter()
        .find(|f| matches!(f.kind, FieldKind::Arena | FieldKind::SkipSlots(_)))
    {
        return Err(Error::new(
            f.span,
            "`arena` and `skip_slots` apply to table fields; an `#[fdb(struct)]` has no \
             slots (use `skip` for a field that is not on the wire)",
        ));
    }
    let krate = &cx.krate;
    let de = de_lifetime();
    let members: Vec<&Field<'_>> = fields
        .iter()
        .filter(|f| f.kind == FieldKind::Field)
        .collect();
    let tys: Vec<&Type> = members.iter().map(|f| f.ty).collect();

    // One assertion per member, spanned at its type so a failure points at the member.
    // (Spanned tokens here only name paths and types, never the impl's locals.)
    let asserts = tys.iter().map(|ty| {
        quote_spanned! {ty.span()=>
            ::core::assert!(
                ::core::matches!(<#ty as #krate::Wire>::SHAPE, #krate::Shape::Inline { .. }),
                "a member of an #[fdb(struct)] must be inline (a scalar, Uid or #[fdb(struct)]); \
                 tables, strings, vectors and unions cannot be struct members"
            );
            ::core::assert!(
                !<#ty as #krate::Wire>::STRUCT,
                "a member of an #[fdb(struct)] cannot be a struct (FDB does not support \
                 nested structs)"
            );
        }
    });
    let shape = quote! {{
        #(#asserts)*
        #krate::Shape::of_struct(&[#(<#tys as #krate::Wire>::SHAPE),*])
    }};
    let wire = impl_wire(
        cx,
        &tys,
        shape,
        Some(quote!(
            const STRUCT: bool = true;
        )),
    );

    let shapes = if members.is_empty() {
        quote!()
    } else {
        quote!(let __fdb_shapes = [#(<#tys as #krate::Wire>::SHAPE),*];)
    };
    let mut encode_steps = Vec::new();
    let mut decode_steps = Vec::new();
    let mut inits = Vec::new();
    let mut index = 0usize;
    for (i, f) in fields.iter().enumerate() {
        let (member, ty) = (&f.member, f.ty);
        let var = format_ident!("__fdb_f{}", i);
        if f.kind == FieldKind::Field {
            encode_steps.push(quote! {
                #krate::__private::encode_struct_member::<#ty>(
                    __fdb_out, &__fdb_shapes, #index, &self.#member,
                )?;
            });
            decode_steps.push(quote! {
                let #var: #ty = #krate::__private::decode_struct_member::<#ty>(
                    __fdb_bytes, &__fdb_shapes, #index,
                )?;
            });
            inits.push(quote!(#member: #var));
            index += 1;
        } else {
            inits.push(default_init(member, ty));
        }
    }

    let absent = absent_inits(cx, fields);
    let (encode_generics, decode_generics) = codec_bounds(cx, fields);
    let name = cx.name;
    let (impl_g, ty_g, _) = cx.generics.split_for_impl();
    let decode_g = decode_generics_params(cx.generics);
    let (decode_impl_g, _, _) = decode_g.split_for_impl();
    let check = shape_check(cx);
    Ok(quote! {
        #wire
        #check

        #[automatically_derived]
        impl #impl_g #krate::Encode for #name #ty_g #encode_generics {
            fn encode_inline(
                &self,
                __fdb_out: &mut [u8],
            ) -> ::core::result::Result<(), #krate::EncodeError> {
                #shapes
                #(#encode_steps)*
                ::core::result::Result::Ok(())
            }
        }

        #[automatically_derived]
        impl #decode_impl_g #krate::Decode<#de> for #name #ty_g #decode_generics {
            fn absent() -> Self {
                Self { #(#absent),* }
            }

            fn decode_inline(
                __fdb_bytes: &#de [u8],
            ) -> ::core::result::Result<Self, #krate::DecodeError> {
                #shapes
                #(#decode_steps)*
                ::core::result::Result::Ok(Self { #(#inits),* })
            }
        }
    })
}

// ---------------------------------------------------------------------------------------
// Unions.

fn expand_union(cx: &Cx<'_>, data: &DataEnum) -> Result<TokenStream> {
    let krate = &cx.krate;
    let de = de_lifetime();
    if data.variants.is_empty() {
        return Err(Error::new_spanned(
            cx.name,
            "a union needs at least one alternative (variant)",
        ));
    }
    if data.variants.len() > usize::from(u8::MAX) {
        return Err(Error::new_spanned(
            cx.name,
            "a union has at most 255 alternatives (the wire tag is a u8)",
        ));
    }
    let mut alternatives = Vec::new();
    let mut errors: Option<Error> = None;
    for v in &data.variants {
        if let Err(e) = attrs::none(&v.attrs, "enum variants") {
            combine(&mut errors, e);
        }
        if let Some((eq, _)) = &v.discriminant {
            combine(
                &mut errors,
                Error::new_spanned(
                    eq,
                    "union tags are the variant index + 1; explicit discriminants are not \
                     supported",
                ),
            );
        }
        let field = match &v.fields {
            Fields::Unnamed(f) if f.unnamed.len() == 1 => &f.unnamed[0],
            _ => {
                combine(
                    &mut errors,
                    Error::new_spanned(
                        v,
                        "each variant of a union must hold exactly one value, like `A(i32)`; \
                         the variant is the alternative and the value its payload (C++ \
                         std::variant alternatives cannot be empty or hold several values)",
                    ),
                );
                continue;
            }
        };
        if let Err(e) = attrs::none(&field.attrs, "union alternatives") {
            combine(&mut errors, e);
        }
        alternatives.push((&v.ident, &field.ty));
    }
    if let Some(e) = errors {
        return Err(e);
    }

    let tys: Vec<&Type> = alternatives.iter().map(|(_, ty)| *ty).collect();
    // One assertion per alternative, spanned at its type (see expand_struct).
    let asserts = tys.iter().map(|ty| {
        quote_spanned! {ty.span()=>
            ::core::assert!(
                <#ty as #krate::Wire>::SHAPE.is_plain(),
                "a union alternative cannot be a union or a vector of unions; wrap it in \
                 EnsureTable"
            );
        }
    });
    let shape = quote! {{
        #(#asserts)*
        #krate::Shape::Union
    }};
    let wire = impl_wire(cx, &tys, shape, None);

    let mut encode_arms = Vec::new();
    let mut decode_arms = Vec::new();
    for (index, (variant, ty)) in alternatives.iter().enumerate() {
        let tag = u8::try_from(index + 1).expect("checked above");
        encode_arms.push(quote! {
            Self::#variant(__fdb_v) => __fdb_w.union_value::<#ty>(#tag, __fdb_v),
        });
        decode_arms.push(quote! {
            #tag => __fdb_r.alternative::<#ty>(__fdb_at).map(Self::#variant),
        });
    }
    let (first, first_ty) = alternatives[0];

    let encode_where = bounds(cx, &[(&tys, quote!(#krate::Encode))]);
    let decode_where = bounds(cx, &[(&tys, quote!(#krate::Decode<#de>))]);
    let name = cx.name;
    let (impl_g, ty_g, _) = cx.generics.split_for_impl();
    let decode_g = decode_generics_params(cx.generics);
    let (decode_impl_g, _, _) = decode_g.split_for_impl();
    let check = shape_check(cx);
    Ok(quote! {
        #wire
        #check

        #[automatically_derived]
        impl #impl_g #krate::Encode for #name #ty_g #encode_where {
            fn encode_union(
                &self,
                __fdb_w: &mut #krate::Writer,
            ) -> ::core::result::Result<#krate::UnionRef, #krate::EncodeError> {
                match self {
                    #(#encode_arms)*
                }
            }
        }

        #[automatically_derived]
        impl #decode_impl_g #krate::Decode<#de> for #name #ty_g #decode_where {
            fn absent() -> Self {
                Self::#first(<#first_ty as #krate::Decode<#de>>::absent())
            }

            fn decode_union(
                __fdb_r: &#krate::Reader<#de>,
                __fdb_tag: u8,
                __fdb_at: usize,
            ) -> ::core::result::Result<Self, #krate::DecodeError> {
                match __fdb_tag {
                    #(#decode_arms)*
                    // C++ reads a tag past the last alternative as the default value.
                    _ => ::core::result::Result::Ok(<Self as #krate::Decode<#de>>::absent()),
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------------------
// Shared pieces.

fn expand_file_identifier(cx: &Cx<'_>, expr: &Expr) -> TokenStream {
    let krate = &cx.krate;
    let name = cx.name;
    let (impl_g, ty_g, where_clause) = cx.generics.split_for_impl();
    let span = expr.span();
    let value = quote_spanned!(span=> #krate::FileIdentifier::new(#expr));
    // Evaluated now, so an out-of-range identifier fails to compile even if unused.
    let check = quote_spanned!(span=> const _: #krate::FileIdentifier = #value;);
    quote! {
        #[automatically_derived]
        impl #impl_g #krate::FileIdentified for #name #ty_g #where_clause {
            const FILE_IDENTIFIER: #krate::FileIdentifier = #value;
        }

        #check
    }
}

/// `impl Wire`. `shape_tys` get a `Wire` bound if they mention a type parameter (the shape
/// of structs and unions depends on their members).
fn impl_wire(
    cx: &Cx<'_>,
    shape_tys: &[&Type],
    shape: TokenStream,
    extra: Option<TokenStream>,
) -> TokenStream {
    let krate = &cx.krate;
    let name = cx.name;
    let (impl_g, ty_g, _) = cx.generics.split_for_impl();
    let where_clause = bounds(cx, &[(shape_tys, quote!(#krate::Wire))]);
    quote! {
        #[automatically_derived]
        impl #impl_g #krate::Wire for #name #ty_g #where_clause {
            const SHAPE: #krate::Shape = #shape;
            #extra
        }
    }
}

/// For a non-generic type (lifetimes aside), a constant that evaluates its shape, so that
/// an invalid struct or union fails to compile where it is defined rather than where it is
/// first encoded.
fn shape_check(cx: &Cx<'_>) -> Option<TokenStream> {
    if cx.generics.type_params().next().is_some() || cx.generics.const_params().next().is_some() {
        return None;
    }
    let krate = &cx.krate;
    let name = cx.name;
    let lifetimes: Vec<TokenStream> = cx.generics.lifetimes().map(|_| quote!('static)).collect();
    let ty = if lifetimes.is_empty() {
        quote!(#name)
    } else {
        quote!(#name<#(#lifetimes),*>)
    };
    Some(quote! {
        const _: #krate::Shape = <#ty as #krate::Wire>::SHAPE;
    })
}

fn default_init(member: &Member, ty: &Type) -> TokenStream {
    quote! {
        #member: <#ty as ::core::default::Default>::default()
    }
}

fn absent_inits(cx: &Cx<'_>, fields: &[Field<'_>]) -> Vec<TokenStream> {
    let krate = &cx.krate;
    let de = de_lifetime();
    fields
        .iter()
        .map(|f| {
            let (member, ty) = (&f.member, f.ty);
            match f.kind {
                FieldKind::Field => quote! {
                    #member: <#ty as #krate::Decode<#de>>::absent()
                },
                _ => default_init(member, ty),
            }
        })
        .collect()
}

/// The where clauses of the `Encode` and `Decode` impls of a table or struct.
fn codec_bounds(cx: &Cx<'_>, fields: &[Field<'_>]) -> (MaybeWhere, MaybeWhere) {
    let krate = &cx.krate;
    let de = de_lifetime();
    let serialized: Vec<&Type> = fields
        .iter()
        .filter(|f| f.kind == FieldKind::Field)
        .map(|f| f.ty)
        .collect();
    let defaulted: Vec<&Type> = fields
        .iter()
        .filter(|f| f.kind != FieldKind::Field)
        .map(|f| f.ty)
        .collect();
    let encode = bounds(cx, &[(&serialized, quote!(#krate::Encode))]);
    let decode = bounds(
        cx,
        &[
            (&serialized, quote!(#krate::Decode<#de>)),
            (&defaulted, quote!(::core::default::Default)),
        ],
    );
    (encode, decode)
}

/// A where clause printed only if it has predicates.
struct MaybeWhere(WhereClause);

impl ToTokens for MaybeWhere {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        if !self.0.predicates.is_empty() {
            self.0.to_tokens(tokens);
        }
    }
}

/// The type's own where clause plus `ty: bound` for each type of each group that mentions
/// a type parameter.
fn bounds(cx: &Cx<'_>, groups: &[(&[&Type], TokenStream)]) -> MaybeWhere {
    let mut clause = cx
        .generics
        .where_clause
        .clone()
        .unwrap_or_else(|| WhereClause {
            where_token: Default::default(),
            predicates: Punctuated::new(),
        });
    let params: Vec<&Ident> = cx.generics.type_params().map(|p| &p.ident).collect();
    for (tys, bound) in groups {
        for &ty in *tys {
            let mut v = MentionsParam {
                params: &params,
                found: false,
            };
            v.visit_type(ty);
            if v.found {
                let predicate: WherePredicate = parse_quote!(#ty: #bound);
                clause.predicates.push(predicate);
            }
        }
    }
    MaybeWhere(clause)
}

struct MentionsParam<'a> {
    params: &'a [&'a Ident],
    found: bool,
}

impl<'ast> Visit<'ast> for MentionsParam<'_> {
    fn visit_path(&mut self, path: &'ast Path) {
        if path.leading_colon.is_none()
            && path
                .segments
                .first()
                .is_some_and(|s| self.params.contains(&&s.ident))
        {
            self.found = true;
        }
        visit::visit_path(self, path);
    }
}

fn de_lifetime() -> syn::Lifetime {
    syn::Lifetime::new(DE, Span::call_site())
}

/// The type's generics with the buffer lifetime added first, outliving every lifetime
/// of the type (`impl<'__fdb_de: 'a, 'a> Decode<'__fdb_de> for T<'a>`).
fn decode_generics_params(generics: &Generics) -> Generics {
    let mut g = generics.clone();
    let mut de = LifetimeParam::new(de_lifetime());
    for lt in generics.lifetimes() {
        de.bounds.push(lt.lifetime.clone());
    }
    g.params.insert(0, GenericParam::Lifetime(de));
    g
}
