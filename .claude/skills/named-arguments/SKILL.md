---
name: named-arguments
description: Design function parameters when a signature has several same-typed positional args, bare bools, optional args, many knobs, overload-like inputs, or variadic input. Replace them with structs, Option, From, enums, trait bounds, builders or slices. Use defaults-and-breakage when the question is which fields are required.
---

# Named arguments at home

Rust has no named arguments. Types do the job better: the names live on the type, not in every function's calling convention.

Source: https://corrode.dev/blog/named-arguments-at-home/

## Pick the shape

| Signature smell | Replace with |
|---|---|
| Several related params, easy to swap (`u32, u32, u32, u32`) | Struct with named fields |
| One optional param | `Option<T>` in the signature |
| Several optional params | Struct of `Option` fields, every field spelled out at the call site |
| Bare `bool` or mode flag | Two-variant enum named for the behavior |
| Fundamentally different input kinds | Enum, with `From` impls for terse call sites |
| Same meaning, different input types | Trait bound: `impl AsRef<str>`, `impl Into<T>` |
| Construction needs validation or conditional steps | Builder |
| Variable count of same-typed args | `&[T]` or `impl IntoIterator<Item = T>` |

## Rules

- Group params into a struct when a caller can swap two of them without a compile error.
- Name the struct for the domain concept it reveals (`Rect`, `ConnectionOptions`), not for the function (`CropArgs`). A missing concept is the usual reason a signature grew.
- Put optionality in the type. No sentinel values (`0`, `-1`, empty string) meaning "not set".
- No `Default` and no `..Default::default()` for param structs. Every field is written at the call site; an explicit `None` is the point. See `defaults-and-breakage` for constructor shape.
- Use `From` impls to convert domain values into param types or enum variants, so call sites stay terse without hiding choices.
- Use field-init shorthand (`Options { timeout, retries }`) so struct construction costs no more than keyword args.
- Enforce invariants in the struct's constructor, not in every function that takes it.
- Reach for a builder last. Prefer a plain struct; build only when a step validates, converts, or depends on a runtime condition. Required values go in `builder(...)`, not in setters.
- Borrow in enum and struct fields where the value is only read for the call (`&str`, not `String`).
- Prefer `impl IntoIterator` over `&[T]` when callers would otherwise collect into a `Vec` just to call.

## Hot paths

- Pass small param structs by value; they are plain stack data and cost nothing over positional args.
- Use generic bounds (`impl Into<T>`), never `Box<dyn Trait>`, for input flexibility.
- No builder that allocates on a hot path. Build once at setup, reuse the result.
