//! Request construction and encoding.

use std::{alloc::Allocator, io, io::Write as _};

use crate::{
    response::{invalid, is_tchar},
    text::Text,
};

/// An HTTP/1.1 request.
///
/// Built up front and handed to [`Fetch::new`](crate::Fetch::new), which
/// encodes it once and sends it. Field values are validated at that point, so a
/// header carrying a stray CRLF fails the request instead of splitting it.
///
/// Every constructor takes the allocator the request owns its bytes in: the
/// method, the target, each header field, and the body all live in `A`.
pub struct Request<A: Allocator + Clone> {
    allocator: A,
    method: Text<A>,
    target: Text<A>,
    headers: Vec<(Text<A>, Text<A>), A>,
    body: Vec<u8, A>,
}

impl<A: Allocator + Clone> Request<A> {
    /// Creates a request for `target`, which must be an origin-form path such
    /// as `/index.html?q=1`.
    pub fn new(allocator: A, method: &str, target: &str) -> Self {
        Self {
            method: Text::from_str_in(method, allocator.clone()),
            target: Text::from_str_in(target, allocator.clone()),
            headers: Vec::new_in(allocator.clone()),
            body: Vec::new_in(allocator.clone()),
            allocator,
        }
    }

    /// Creates a `GET` request.
    pub fn get(allocator: A, target: &str) -> Self {
        Self::new(allocator, "GET", target)
    }

    /// Creates a `HEAD` request, whose response carries headers but no body.
    pub fn head(allocator: A, target: &str) -> Self {
        Self::new(allocator, "HEAD", target)
    }

    /// Creates a `POST` request carrying `body`.
    pub fn post(allocator: A, target: &str, body: Vec<u8, A>) -> Self {
        Self::new(allocator, "POST", target).with_body(body)
    }

    /// Creates a `PUT` request carrying `body`.
    pub fn put(allocator: A, target: &str, body: Vec<u8, A>) -> Self {
        Self::new(allocator, "PUT", target).with_body(body)
    }

    /// Creates a `DELETE` request.
    pub fn delete(allocator: A, target: &str) -> Self {
        Self::new(allocator, "DELETE", target)
    }

    /// Adds a header field. Fields are sent in the order they are added, and
    /// one added here overrides the default this client would otherwise send.
    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((
            Text::from_str_in(name, self.allocator.clone()),
            Text::from_str_in(value, self.allocator.clone()),
        ));
        self
    }

    /// Sets the request body, which must already be allocated in `A`.
    pub fn with_body(mut self, body: Vec<u8, A>) -> Self {
        self.body = body;
        self
    }

    /// Returns the request method.
    pub fn method(&self) -> &str {
        &self.method
    }

    /// Returns the request target.
    pub fn target(&self) -> &str {
        &self.target
    }

    /// Returns the request body.
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// Returns the allocator the request owns its bytes in.
    pub fn allocator(&self) -> &A {
        &self.allocator
    }

    /// True when this request is a `HEAD`, whose response has no body.
    pub fn is_head(&self) -> bool {
        self.method.eq_ignore_ascii_case("HEAD")
    }

    /// Encodes the request into the bytes to put on the wire.
    ///
    /// Supplies three defaults the caller did not set: `Host` (required by
    /// HTTP/1.1, defaulted to the peer address), `Content-Length` when the
    /// request carries content, and `Connection: close`, since this client
    /// uses one connection per request.
    ///
    /// The returned buffer is allocated in `A`, sized up front so the whole
    /// request costs a single allocation.
    pub fn encode(&self, default_host: &str) -> io::Result<Vec<u8, A>> {
        self.validate()?;

        let mut out =
            Vec::with_capacity_in(self.encoded_size_hint(default_host), self.allocator.clone());
        // Writing to a `Vec` never fails, so the results are discarded the way
        // `write!` to a `String` would be.
        let _ = write!(out, "{} {} HTTP/1.1\r\n", self.method, self.target);
        if !self.has_header("host") {
            let _ = write!(out, "Host: {default_host}\r\n");
        }
        for (name, value) in &self.headers {
            let _ = write!(out, "{name}: {value}\r\n");
        }
        if !self.has_header("content-length") && self.sends_content() {
            let _ = write!(out, "Content-Length: {}\r\n", self.body.len());
        }
        if !self.has_header("connection") {
            out.extend_from_slice(b"Connection: close\r\n");
        }
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(&self.body);
        Ok(out)
    }

    /// Upper bound on the encoded size, so [`Request::encode`] allocates once.
    ///
    /// The three defaults are counted whether or not they are emitted: a few
    /// unused bytes of capacity are cheaper than a realloc.
    fn encoded_size_hint(&self, default_host: &str) -> usize {
        /// `Host: ` + `\r\n`, `Content-Length: ` + 20 digits + `\r\n`, and
        /// `Connection: close\r\n`.
        const DEFAULTS: usize = 8 + 38 + 19;

        let headers: usize = self
            .headers
            .iter()
            .map(|(name, value)| name.len() + value.len() + 4)
            .sum();
        self.method.len()
            + self.target.len()
            + "  HTTP/1.1\r\n\r\n".len()
            + default_host.len()
            + DEFAULTS
            + headers
            + self.body.len()
    }

    /// True when the request should announce a body length, which methods
    /// defined to carry content do even when that content is empty.
    fn sends_content(&self) -> bool {
        !self.body.is_empty()
            || ["POST", "PUT", "PATCH"]
                .iter()
                .any(|method| self.method.eq_ignore_ascii_case(method))
    }

    fn has_header(&self, name: &str) -> bool {
        self.headers
            .iter()
            .any(|(field, _)| field.eq_ignore_ascii_case(name))
    }

    /// Rejects anything that would produce a malformed or ambiguous request.
    fn validate(&self) -> io::Result<()> {
        if self.method.is_empty() || !self.method.bytes().all(is_tchar) {
            return Err(invalid("malformed request method"));
        }
        if !self.target.starts_with('/') {
            return Err(invalid("request target must be an origin-form path"));
        }
        if self
            .target
            .bytes()
            .any(|b| b <= b' ' || b == 0x7f || b >= 0x80)
        {
            return Err(invalid("malformed request target"));
        }
        for (name, value) in &self.headers {
            if name.is_empty() || !name.bytes().all(is_tchar) {
                return Err(invalid("malformed header field name"));
            }
            if value
                .bytes()
                .any(|b| b == b'\r' || b == b'\n' || b == 0 || (b < b' ' && b != b'\t'))
            {
                return Err(invalid("malformed header field value"));
            }
        }
        Ok(())
    }
}

impl<A: Allocator + Clone> Clone for Request<A> {
    fn clone(&self) -> Self {
        Self {
            method: self.method.clone(),
            target: self.target.clone(),
            headers: self.headers.clone(),
            body: self.body.clone(),
            allocator: self.allocator.clone(),
        }
    }
}

impl<A: Allocator + Clone> std::fmt::Debug for Request<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Request")
            .field("method", &self.method)
            .field("target", &self.target)
            .field("headers", &self.headers)
            .field("body", &self.body.len())
            .finish()
    }
}

impl<A: Allocator + Clone, B: Allocator + Clone> PartialEq<Request<B>> for Request<A> {
    fn eq(&self, other: &Request<B>) -> bool {
        self.method == other.method
            && self.target == other.target
            && self.body == other.body
            && self.headers.len() == other.headers.len()
            && self
                .headers
                .iter()
                .zip(other.headers.iter())
                .all(|((ln, lv), (rn, rv))| ln == rn && lv == rv)
    }
}

impl<A: Allocator + Clone> Eq for Request<A> {}

#[cfg(test)]
mod tests {
    use std::alloc::Global;

    use super::*;

    fn encode(request: Request<Global>) -> String {
        String::from_utf8(request.encode("example.com:8080").expect("encode").to_vec()).unwrap()
    }

    fn body(bytes: &[u8]) -> Vec<u8, Global> {
        let mut body = Vec::new_in(Global);
        body.extend_from_slice(bytes);
        body
    }

    #[test]
    fn get_carries_the_defaults() {
        assert_eq!(
            encode(Request::get(Global, "/index.html")),
            "GET /index.html HTTP/1.1\r\n\
             Host: example.com:8080\r\n\
             Connection: close\r\n\r\n"
        );
    }

    #[test]
    fn post_announces_its_content_length() {
        assert_eq!(
            encode(Request::post(Global, "/api", body(b"hi"))),
            "POST /api HTTP/1.1\r\n\
             Host: example.com:8080\r\n\
             Content-Length: 2\r\n\
             Connection: close\r\n\r\nhi"
        );
    }

    #[test]
    fn empty_post_still_announces_zero_length() {
        assert!(
            encode(Request::post(Global, "/api", Vec::new_in(Global)))
                .contains("Content-Length: 0\r\n")
        );
        assert!(!encode(Request::get(Global, "/")).contains("Content-Length"));
    }

    #[test]
    fn caller_headers_override_the_defaults() {
        let encoded = encode(
            Request::get(Global, "/")
                .with_header("Host", "override.example")
                .with_header("Connection", "keep-alive")
                .with_header("Accept", "*/*"),
        );
        assert_eq!(encoded.matches("Host:").count(), 1);
        assert!(encoded.contains("Host: override.example\r\n"));
        assert!(encoded.contains("Connection: keep-alive\r\n"));
        assert!(!encoded.contains("Connection: close"));
        assert!(encoded.contains("Accept: */*\r\n"));
    }

    #[test]
    fn the_size_hint_covers_the_encoding() {
        let request = Request::post(Global, "/api/items", body(&[b'x'; 256]))
            .with_header("Content-Type", "application/json");
        let host = "example.com:8080";
        let encoded = request.encode(host).expect("encode");
        assert!(encoded.len() <= request.encoded_size_hint(host));
        // A single allocation, sized once: the buffer never grew.
        assert_eq!(encoded.capacity(), request.encoded_size_hint(host));
    }

    #[test]
    fn rejects_requests_that_would_be_malformed() {
        let host = "example.com";
        assert!(Request::get(Global, "index.html").encode(host).is_err());
        assert!(Request::get(Global, "/a b").encode(host).is_err());
        assert!(
            Request::new(Global, "BAD METHOD", "/")
                .encode(host)
                .is_err()
        );
        assert!(
            Request::get(Global, "/")
                .with_header("X", "a\r\nInjected: 1")
                .encode(host)
                .is_err()
        );
        assert!(
            Request::get(Global, "/")
                .with_header("Bad Name", "1")
                .encode(host)
                .is_err()
        );
    }
}
