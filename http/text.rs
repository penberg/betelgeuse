//! An allocator-aware UTF-8 string.
//!
//! [`String`](std::string::String) is hard-wired to the global allocator, so
//! every method, target, reason phrase, and header field this crate owns is
//! stored in a [`Text`] instead: a `Vec<u8, A>` that upholds the same UTF-8
//! invariant a `String` does, allocated through the caller's allocator.

use std::{alloc::Allocator, fmt, ops::Deref};

/// A UTF-8 string whose bytes are allocated in `A`.
///
/// Every constructor either takes a `&str` or replaces invalid sequences, so
/// the bytes are always valid UTF-8 and [`Text::as_str`] is free.
pub struct Text<A: Allocator> {
    bytes: Vec<u8, A>,
}

impl<A: Allocator> Text<A> {
    /// Creates an empty string in `allocator`.
    pub fn new_in(allocator: A) -> Self {
        Self {
            bytes: Vec::new_in(allocator),
        }
    }

    /// Copies `text` into `allocator`.
    pub fn from_str_in(text: &str, allocator: A) -> Self {
        let mut bytes = Vec::with_capacity_in(text.len(), allocator);
        bytes.extend_from_slice(text.as_bytes());
        Self { bytes }
    }

    /// Copies `bytes` into `allocator`, replacing each invalid UTF-8 sequence
    /// with `U+FFFD`.
    ///
    /// This is [`String::from_utf8_lossy`](std::string::String::from_utf8_lossy)
    /// without its global allocation: peers are free to put arbitrary bytes in
    /// a header field value, and the crate copies them exactly once, into the
    /// allocator the caller chose.
    pub fn from_utf8_lossy_in(bytes: &[u8], allocator: A) -> Self {
        const REPLACEMENT: &str = "\u{fffd}";

        let mut out = Vec::with_capacity_in(bytes.len(), allocator);
        let mut rest = bytes;
        loop {
            match std::str::from_utf8(rest) {
                Ok(valid) => {
                    out.extend_from_slice(valid.as_bytes());
                    break;
                }
                Err(error) => {
                    let (valid, invalid) = rest.split_at(error.valid_up_to());
                    out.extend_from_slice(valid);
                    out.extend_from_slice(REPLACEMENT.as_bytes());
                    match error.error_len() {
                        Some(len) => rest = &invalid[len..],
                        // The input ends mid-sequence: nothing left to scan.
                        None => break,
                    }
                }
            }
        }
        Self { bytes: out }
    }

    /// Returns the string.
    pub fn as_str(&self) -> &str {
        // SAFETY: every constructor stores either a `&str` or bytes passed
        // through the lossy conversion above, so `bytes` is valid UTF-8.
        unsafe { std::str::from_utf8_unchecked(&self.bytes) }
    }

    /// Returns the underlying bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the allocator the string is stored in.
    pub fn allocator(&self) -> &A {
        self.bytes.allocator()
    }
}

impl<A: Allocator> Deref for Text<A> {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl<A: Allocator> AsRef<str> for Text<A> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl<A: Allocator + Clone> Clone for Text<A> {
    fn clone(&self) -> Self {
        Self {
            bytes: self.bytes.clone(),
        }
    }
}

impl<A: Allocator> fmt::Debug for Text<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

impl<A: Allocator> fmt::Display for Text<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// Equality is over the text, not the allocator: two strings with the same
// bytes are equal however they were allocated.
impl<A: Allocator, B: Allocator> PartialEq<Text<B>> for Text<A> {
    fn eq(&self, other: &Text<B>) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl<A: Allocator> Eq for Text<A> {}

impl<A: Allocator> PartialEq<str> for Text<A> {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl<A: Allocator> PartialEq<&str> for Text<A> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl<A: Allocator> PartialEq<Text<A>> for str {
    fn eq(&self, other: &Text<A>) -> bool {
        self == other.as_str()
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::Global;

    use super::*;

    #[test]
    fn copies_a_str() {
        let text = Text::from_str_in("hello", Global);
        assert_eq!(text.as_str(), "hello");
        assert_eq!(text, "hello");
        assert_eq!(text.len(), 5);
    }

    #[test]
    fn replaces_invalid_sequences() {
        assert_eq!(
            Text::from_utf8_lossy_in(b"a\xffb", Global).as_str(),
            "a\u{fffd}b"
        );
        assert_eq!(
            Text::from_utf8_lossy_in(b"tail\xe2\x82", Global).as_str(),
            "tail\u{fffd}"
        );
        assert_eq!(
            Text::from_utf8_lossy_in("héllo".as_bytes(), Global).as_str(),
            "héllo"
        );
    }

    #[test]
    fn empty_text_is_empty() {
        let text = Text::new_in(Global);
        assert!(text.is_empty());
        assert_eq!(text, "");
    }
}
