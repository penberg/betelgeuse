//! Response head parsing and the response value handed back to the caller.

use std::{alloc::Allocator, io};

use crate::text::Text;

/// Default ceiling on the size of a response head.
pub const DEFAULT_MAX_HEAD_BYTES: usize = 64 * 1024;

/// A parsed set of HTTP header fields, in the order the peer sent them.
///
/// Lookups are case-insensitive, as required by HTTP/1.1. A field may appear
/// more than once; [`Headers::get`] returns the first occurrence and
/// [`Headers::get_all`] returns every one.
///
/// Field names and values are owned by the allocator the request was made
/// with, not by the global one.
pub struct Headers<A: Allocator + Clone> {
    fields: Vec<(Text<A>, Text<A>), A>,
}

impl<A: Allocator + Clone> Headers<A> {
    /// Creates an empty set of fields in `allocator`.
    pub fn new_in(allocator: A) -> Self {
        Self {
            fields: Vec::new_in(allocator),
        }
    }

    /// Returns the value of the first field named `name`.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(field, _)| field.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// Returns the values of every field named `name`.
    pub fn get_all<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        self.fields
            .iter()
            .filter(move |(field, _)| field.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// Returns true when at least one field named `name` is present.
    pub fn contains(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    /// Iterates over every field as a `(name, value)` pair.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.fields
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    /// Returns the number of fields, counting repeats separately.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Returns true when the response carried no header fields.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Returns the allocator the fields are stored in.
    pub fn allocator(&self) -> &A {
        self.fields.allocator()
    }
}

impl<A: Allocator + Clone> Clone for Headers<A> {
    fn clone(&self) -> Self {
        Self {
            fields: self.fields.clone(),
        }
    }
}

impl<A: Allocator + Clone> std::fmt::Debug for Headers<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<A: Allocator + Clone, B: Allocator + Clone> PartialEq<Headers<B>> for Headers<A> {
    fn eq(&self, other: &Headers<B>) -> bool {
        self.fields.len() == other.fields.len()
            && self
                .fields
                .iter()
                .zip(other.fields.iter())
                .all(|((ln, lv), (rn, rv))| ln == rn && lv == rv)
    }
}

impl<A: Allocator + Clone> Eq for Headers<A> {}

/// The status line and header fields of a response, without its body.
pub struct Head<A: Allocator + Clone> {
    pub status: u16,
    pub reason: Text<A>,
    pub headers: Headers<A>,
}

/// A complete HTTP response with its body buffered in memory.
///
/// Every byte the response owns — the reason phrase, the header fields, and
/// the body — is allocated in `A`.
pub struct Response<A: Allocator + Clone> {
    status: u16,
    reason: Text<A>,
    headers: Headers<A>,
    body: Vec<u8, A>,
}

impl<A: Allocator + Clone> Response<A> {
    /// Assembles a response from its parts.
    pub fn new(status: u16, reason: Text<A>, headers: Headers<A>, body: Vec<u8, A>) -> Self {
        Self {
            status,
            reason,
            headers,
            body,
        }
    }

    /// Returns the status code.
    pub fn status(&self) -> u16 {
        self.status
    }

    /// Returns the reason phrase, which servers are allowed to leave empty.
    pub fn reason(&self) -> &str {
        &self.reason
    }

    /// Returns every header field.
    pub fn headers(&self) -> &Headers<A> {
        &self.headers
    }

    /// Returns the value of the first header field named `name`.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name)
    }

    /// Returns the response body.
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// Consumes the response and returns its body, still allocated in `A`.
    pub fn into_body(self) -> Vec<u8, A> {
        self.body
    }

    /// Returns the allocator the response owns its bytes in.
    pub fn allocator(&self) -> &A {
        self.body.allocator()
    }

    /// Returns true for a 2xx status code.
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

impl<A: Allocator + Clone> Clone for Response<A> {
    fn clone(&self) -> Self {
        Self {
            status: self.status,
            reason: self.reason.clone(),
            headers: self.headers.clone(),
            body: self.body.clone(),
        }
    }
}

impl<A: Allocator + Clone> std::fmt::Debug for Response<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Response")
            .field("status", &self.status)
            .field("reason", &self.reason)
            .field("headers", &self.headers)
            .field("body", &self.body.len())
            .finish()
    }
}

impl<A: Allocator + Clone, B: Allocator + Clone> PartialEq<Response<B>> for Response<A> {
    fn eq(&self, other: &Response<B>) -> bool {
        self.status == other.status
            && self.reason == other.reason
            && self.headers == other.headers
            && self.body == other.body
    }
}

impl<A: Allocator + Clone> Eq for Response<A> {}

/// Parses a response head out of `buf`, allocating its text in `allocator`.
///
/// Returns `Ok(None)` while the terminating empty line has not arrived yet, and
/// otherwise the parsed head plus the number of bytes it occupied in `buf`.
/// Fails once more than `max_head_bytes` have arrived without that line.
pub fn parse_head<A: Allocator + Clone>(
    buf: &[u8],
    max_head_bytes: usize,
    allocator: A,
) -> io::Result<Option<(Head<A>, usize)>> {
    let Some(end) = find(buf, b"\r\n\r\n") else {
        if buf.len() > max_head_bytes {
            return Err(invalid("response head too large"));
        }
        return Ok(None);
    };

    let mut lines = crlf_lines(&buf[..end]);
    let status_line = lines.next().ok_or_else(|| invalid("empty response head"))?;
    let (status, reason) = parse_status_line(status_line, allocator.clone())?;

    let mut fields = Vec::new_in(allocator.clone());
    for line in lines {
        fields.push(parse_header_line(line, allocator.clone())?);
    }

    Ok(Some((
        Head {
            status,
            reason,
            headers: Headers { fields },
        },
        end + 4,
    )))
}

/// Parses `HTTP/1.x <status> [reason]`.
fn parse_status_line<A: Allocator + Clone>(
    line: &[u8],
    allocator: A,
) -> io::Result<(u16, Text<A>)> {
    let mut parts = line.splitn(3, |b| *b == b' ');

    let version = parts
        .next()
        .ok_or_else(|| invalid("missing http version"))?;
    if !version.starts_with(b"HTTP/1.") {
        return Err(invalid("unsupported http version"));
    }

    let status = parts.next().ok_or_else(|| invalid("missing status code"))?;
    if status.len() != 3 || !status.iter().all(u8::is_ascii_digit) {
        return Err(invalid("malformed status code"));
    }
    let status = status
        .iter()
        .fold(0_u16, |acc, digit| acc * 10 + u16::from(digit - b'0'));

    let reason = trim_ascii_whitespace(parts.next().unwrap_or(b""));
    Ok((status, Text::from_utf8_lossy_in(reason, allocator)))
}

/// Parses one `Name: value` field line.
fn parse_header_line<A: Allocator + Clone>(
    line: &[u8],
    allocator: A,
) -> io::Result<(Text<A>, Text<A>)> {
    if line.starts_with(b" ") || line.starts_with(b"\t") {
        return Err(invalid("obsolete line folding in response header"));
    }

    let colon = line
        .iter()
        .position(|b| *b == b':')
        .ok_or_else(|| invalid("header field without a colon"))?;
    let name = &line[..colon];
    if name.is_empty() || !name.iter().all(|b| is_tchar(*b)) {
        return Err(invalid("malformed header field name"));
    }

    let value = trim_optional_whitespace(&line[colon + 1..]);
    Ok((
        Text::from_utf8_lossy_in(name, allocator.clone()),
        Text::from_utf8_lossy_in(value, allocator),
    ))
}

/// Trims the spaces and horizontal tabs a peer may pad a field value with
/// (RFC 9112 `OWS`).
fn trim_optional_whitespace(mut value: &[u8]) -> &[u8] {
    while let [b' ' | b'\t', rest @ ..] = value {
        value = rest;
    }
    while let [rest @ .., b' ' | b'\t'] = value {
        value = rest;
    }
    value
}

/// Trims any ASCII whitespace, which a reason phrase may carry on either side.
fn trim_ascii_whitespace(mut text: &[u8]) -> &[u8] {
    while let [first, rest @ ..] = text
        && first.is_ascii_whitespace()
    {
        text = rest;
    }
    while let [rest @ .., last] = text
        && last.is_ascii_whitespace()
    {
        text = rest;
    }
    text
}

/// Splits `text` on CRLF, yielding the lines without their terminators.
fn crlf_lines(text: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut rest = text;
    std::iter::from_fn(move || {
        if rest.is_empty() {
            return None;
        }
        match find(rest, b"\r\n") {
            Some(at) => {
                let line = &rest[..at];
                rest = &rest[at + 2..];
                Some(line)
            }
            None => {
                let line = rest;
                rest = &[];
                Some(line)
            }
        }
    })
}

/// Returns the offset of the first occurrence of `needle` in `haystack`.
pub fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.len() > haystack.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// True for a character allowed in a header field name (RFC 9110 `tchar`).
pub fn is_tchar(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

/// Builds the error used for every protocol-level failure in this crate.
///
/// The message is a `&'static str`, so the crate itself copies nothing: the
/// only allocation left is the one `io::Error` makes internally, which std
/// performs in the global allocator and does not expose a hook for.
pub fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use std::alloc::Global;

    use super::*;

    fn parse(buf: &[u8]) -> io::Result<Option<(Head<Global>, usize)>> {
        parse_head(buf, DEFAULT_MAX_HEAD_BYTES, Global)
    }

    #[test]
    fn parses_status_line_and_fields() {
        let (head, consumed) = parse(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nX-A: 1\r\n\r\nabc")
            .unwrap()
            .unwrap();
        assert_eq!(head.status, 200);
        assert_eq!(head.reason, "OK");
        assert_eq!(head.headers.get("content-length"), Some("3"));
        assert_eq!(head.headers.get("X-a"), Some("1"));
        assert_eq!(consumed, 46);
    }

    #[test]
    fn head_is_incomplete_until_the_empty_line() {
        assert!(
            parse(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn accepts_an_empty_reason_phrase() {
        let (head, _) = parse(b"HTTP/1.1 204 \r\n\r\n").unwrap().unwrap();
        assert_eq!(head.status, 204);
        assert_eq!(head.reason, "");
        assert!(head.headers.is_empty());
    }

    #[test]
    fn field_values_need_not_be_utf8() {
        let (head, _) = parse(b"HTTP/1.1 200 OK\r\nX-Raw: a\xffb\r\n\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(head.headers.get("x-raw"), Some("a\u{fffd}b"));
    }

    #[test]
    fn repeated_fields_are_all_retained() {
        let (head, _) = parse(b"HTTP/1.1 200 OK\r\nSet-Cookie: a\r\nSet-Cookie: b\r\n\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(
            head.headers.get_all("set-cookie").collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }

    #[test]
    fn rejects_malformed_heads() {
        for head in [
            &b"ICY 200 OK\r\n\r\n"[..],
            &b"HTTP/1.1 20 OK\r\n\r\n"[..],
            &b"HTTP/1.1 2000 OK\r\n\r\n"[..],
            &b"HTTP/1.1 200 OK\r\nNo-Colon\r\n\r\n"[..],
            &b"HTTP/1.1 200 OK\r\nBad Name: 1\r\n\r\n"[..],
            &b"HTTP/1.1 200 OK\r\nA: 1\r\n continued\r\n\r\n"[..],
        ] {
            assert!(parse(head).is_err(), "expected rejection: {head:?}");
        }
    }

    #[test]
    fn rejects_an_oversized_head() {
        let mut buf = b"HTTP/1.1 200 OK\r\n".to_vec();
        buf.resize(DEFAULT_MAX_HEAD_BYTES + 1, b'x');
        assert!(parse(&buf).is_err());
    }
}
