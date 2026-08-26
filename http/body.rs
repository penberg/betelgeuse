//! Response body framing.
//!
//! HTTP/1.1 delimits a response body in one of four ways, and which one applies
//! is decided entirely by the response head (RFC 9112 §6.3). [`BodyDecoder`]
//! captures that decision once and then consumes bytes incrementally, so a body
//! may arrive split across any number of `recv` completions.

use std::{alloc::Allocator, io};

use crate::response::{Head, Headers, find, invalid};

/// Largest chunk-size or trailer line accepted in a chunked body.
const MAX_CHUNK_LINE: usize = 1024;

/// Whether the body is complete after the bytes consumed so far.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyStatus {
    NeedMore,
    Complete,
}

pub enum BodyDecoder {
    /// The status line forbids a body: HEAD, 1xx, 204, or 304.
    Empty,
    /// `Content-Length` gave an exact size.
    Length { remaining: u64 },
    /// `Transfer-Encoding: chunked`.
    Chunked { state: ChunkState },
    /// Neither framing header is present, so the body ends when the peer
    /// closes the connection.
    UntilEof,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkState {
    Size,
    Data { remaining: u64 },
    DataCrlf,
    Trailer,
    Done,
}

impl BodyDecoder {
    /// Chooses the framing implied by a response head.
    ///
    /// `head_request` suppresses the body for a response to `HEAD`, which
    /// carries the same headers as the equivalent `GET` but no content.
    pub fn for_response<A: Allocator + Clone>(
        head: &Head<A>,
        head_request: bool,
    ) -> io::Result<Self> {
        if head_request || head.status == 204 || head.status == 304 || head.status < 200 {
            return Ok(Self::Empty);
        }

        // Chunked wins over Content-Length, which must be ignored when both
        // are present (RFC 9112 §6.3).
        if let Some(encoding) = head.headers.get("transfer-encoding") {
            let chunked = encoding
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("chunked"));
            if !chunked {
                return Err(invalid("unsupported transfer-encoding"));
            }
            return Ok(Self::Chunked {
                state: ChunkState::Size,
            });
        }

        match content_length(&head.headers)? {
            Some(remaining) => Ok(Self::Length { remaining }),
            None => Ok(Self::UntilEof),
        }
    }

    /// Consumes as much of `input` as belongs to the body, appending the
    /// decoded bytes to `out`.
    ///
    /// Returns how many bytes of `input` were consumed and whether the body is
    /// now complete. Bytes left unconsumed are framing that needs more input
    /// before it can be interpreted.
    /// `out` is the caller's buffer, so decoded bytes land in the caller's
    /// allocator: the decoder itself owns no heap memory.
    pub fn decode<A: Allocator>(
        &mut self,
        input: &[u8],
        out: &mut Vec<u8, A>,
    ) -> io::Result<(usize, BodyStatus)> {
        match self {
            Self::Empty => Ok((0, BodyStatus::Complete)),
            Self::Length { remaining } => {
                let take = (*remaining).min(input.len() as u64) as usize;
                out.extend_from_slice(&input[..take]);
                *remaining -= take as u64;
                let status = if *remaining == 0 {
                    BodyStatus::Complete
                } else {
                    BodyStatus::NeedMore
                };
                Ok((take, status))
            }
            Self::UntilEof => {
                out.extend_from_slice(input);
                Ok((input.len(), BodyStatus::NeedMore))
            }
            Self::Chunked { state } => decode_chunked(state, input, out),
        }
    }

    /// Reports what a clean connection close means for this body.
    ///
    /// Close-delimited and body-less responses are complete; a truncated
    /// `Content-Length` or chunked body is an error.
    pub fn at_eof(&self) -> io::Result<()> {
        match self {
            Self::Empty | Self::UntilEof => Ok(()),
            Self::Length { remaining: 0 } => Ok(()),
            Self::Length { .. } => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed before the full content-length body arrived",
            )),
            Self::Chunked {
                state: ChunkState::Done,
            } => Ok(()),
            Self::Chunked { .. } => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed inside a chunked body",
            )),
        }
    }
}

/// Reads `Content-Length`, rejecting repeated fields that disagree.
fn content_length<A: Allocator + Clone>(headers: &Headers<A>) -> io::Result<Option<u64>> {
    let mut length = None;
    for value in headers.get_all("content-length") {
        let parsed: u64 = value
            .trim()
            .parse()
            .map_err(|_| invalid("malformed content-length"))?;
        match length {
            Some(previous) if previous != parsed => {
                return Err(invalid("conflicting content-length fields"));
            }
            _ => length = Some(parsed),
        }
    }
    Ok(length)
}

fn decode_chunked<A: Allocator>(
    state: &mut ChunkState,
    input: &[u8],
    out: &mut Vec<u8, A>,
) -> io::Result<(usize, BodyStatus)> {
    let mut consumed = 0;
    loop {
        match *state {
            ChunkState::Size => {
                let Some(line) = next_line(&input[consumed..])? else {
                    return Ok((consumed, BodyStatus::NeedMore));
                };
                let size = parse_chunk_size(&input[consumed..consumed + line])?;
                consumed += line + 2;
                *state = if size == 0 {
                    ChunkState::Trailer
                } else {
                    ChunkState::Data { remaining: size }
                };
            }
            ChunkState::Data { remaining } => {
                let available = input.len() - consumed;
                if available == 0 {
                    return Ok((consumed, BodyStatus::NeedMore));
                }
                let take = remaining.min(available as u64) as usize;
                out.extend_from_slice(&input[consumed..consumed + take]);
                consumed += take;
                *state = match remaining - take as u64 {
                    0 => ChunkState::DataCrlf,
                    left => ChunkState::Data { remaining: left },
                };
            }
            ChunkState::DataCrlf => {
                if input.len() - consumed < 2 {
                    return Ok((consumed, BodyStatus::NeedMore));
                }
                if &input[consumed..consumed + 2] != b"\r\n" {
                    return Err(invalid("chunk data not terminated by CRLF"));
                }
                consumed += 2;
                *state = ChunkState::Size;
            }
            ChunkState::Trailer => {
                let Some(line) = next_line(&input[consumed..])? else {
                    return Ok((consumed, BodyStatus::NeedMore));
                };
                consumed += line + 2;
                // An empty line ends the trailer section; anything else is a
                // trailer field, which this client discards.
                if line == 0 {
                    *state = ChunkState::Done;
                }
            }
            ChunkState::Done => return Ok((consumed, BodyStatus::Complete)),
        }
    }
}

/// Returns the length of the next CRLF-terminated line in `input`, excluding
/// the terminator, or `None` while the line is incomplete.
fn next_line(input: &[u8]) -> io::Result<Option<usize>> {
    match find(input, b"\r\n") {
        Some(at) => Ok(Some(at)),
        None if input.len() > MAX_CHUNK_LINE => Err(invalid("chunk line too long")),
        None => Ok(None),
    }
}

/// Parses a chunk size: hexadecimal, optionally followed by `;extension`.
fn parse_chunk_size(line: &[u8]) -> io::Result<u64> {
    let digits = match line.iter().position(|b| *b == b';') {
        Some(at) => &line[..at],
        None => line,
    };
    if digits.is_empty() || digits.len() > 16 {
        return Err(invalid("malformed chunk size"));
    }

    let mut size = 0_u64;
    for digit in digits {
        let value = (*digit as char)
            .to_digit(16)
            .ok_or_else(|| invalid("malformed chunk size"))?;
        size = size * 16 + u64::from(value);
    }
    Ok(size)
}

#[cfg(test)]
mod tests {
    use std::alloc::Global;

    use super::*;

    fn chunked() -> BodyDecoder {
        BodyDecoder::Chunked {
            state: ChunkState::Size,
        }
    }

    fn head(status: u16, fields: &[(&str, &str)]) -> Head<Global> {
        let raw: String = fields
            .iter()
            .map(|(name, value)| format!("{name}: {value}\r\n"))
            .collect();
        let response = format!("HTTP/1.1 {status} X\r\n{raw}\r\n");
        crate::response::parse_head(
            response.as_bytes(),
            crate::response::DEFAULT_MAX_HEAD_BYTES,
            Global,
        )
        .unwrap()
        .unwrap()
        .0
    }

    #[test]
    fn content_length_body_completes_exactly() {
        let mut decoder = BodyDecoder::for_response(&head(200, &[("content-length", "5")]), false)
            .expect("framing");
        let mut out = Vec::new_in(Global);
        assert_eq!(
            decoder.decode(b"helloXXX", &mut out).unwrap(),
            (5, BodyStatus::Complete)
        );
        assert_eq!(out, b"hello");
    }

    #[test]
    fn chunked_body_reassembles_across_reads() {
        let mut decoder = chunked();
        let mut out = Vec::new_in(Global);
        let input = b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";

        // Grow the window one byte at a time, the way the rx buffer does:
        // the decoder must resume at any split point and leave incomplete
        // framing lines unconsumed until the rest arrives.
        let mut consumed = 0;
        let mut status = BodyStatus::NeedMore;
        for end in 1..=input.len() {
            let (used, next) = decoder.decode(&input[consumed..end], &mut out).unwrap();
            consumed += used;
            status = next;
        }
        assert_eq!(consumed, input.len());
        assert_eq!(status, BodyStatus::Complete);
        assert_eq!(out, b"hello world");
    }

    #[test]
    fn chunked_body_accepts_extensions_and_trailers() {
        let mut decoder = chunked();
        let mut out = Vec::new_in(Global);
        let input = b"3;name=value\r\nabc\r\n0\r\nX-Trailer: 1\r\n\r\n";
        let (consumed, status) = decoder.decode(input, &mut out).unwrap();
        assert_eq!(status, BodyStatus::Complete);
        assert_eq!(consumed, input.len());
        assert_eq!(out, b"abc");
    }

    #[test]
    fn chunked_rejects_a_bad_size_and_a_missing_crlf() {
        let mut out = Vec::new_in(Global);
        assert!(chunked().decode(b"zz\r\nabc\r\n", &mut out).is_err());
        assert!(chunked().decode(b"3\r\nabcXX", &mut out).is_err());
    }

    #[test]
    fn truncated_bodies_fail_at_eof() {
        let mut out = Vec::new_in(Global);

        let mut length = BodyDecoder::Length { remaining: 4 };
        length.decode(b"ab", &mut out).unwrap();
        assert!(length.at_eof().is_err());

        let mut partial = chunked();
        partial.decode(b"5\r\nhel", &mut out).unwrap();
        assert!(partial.at_eof().is_err());

        assert!(BodyDecoder::UntilEof.at_eof().is_ok());
        assert!(BodyDecoder::Empty.at_eof().is_ok());
    }

    #[test]
    fn statuses_without_a_body_are_empty() {
        for status in [100, 204, 304] {
            let decoder =
                BodyDecoder::for_response(&head(status, &[("content-length", "5")]), false)
                    .unwrap();
            assert!(matches!(decoder, BodyDecoder::Empty));
        }
        let decoder =
            BodyDecoder::for_response(&head(200, &[("content-length", "5")]), true).unwrap();
        assert!(matches!(decoder, BodyDecoder::Empty));
    }

    #[test]
    fn framing_headers_pick_the_decoder() {
        assert!(matches!(
            BodyDecoder::for_response(&head(200, &[]), false).unwrap(),
            BodyDecoder::UntilEof
        ));
        assert!(matches!(
            BodyDecoder::for_response(&head(200, &[("transfer-encoding", "chunked")]), false)
                .unwrap(),
            BodyDecoder::Chunked { .. }
        ));
        // Chunked framing wins when a server sends both.
        assert!(matches!(
            BodyDecoder::for_response(
                &head(
                    200,
                    &[("content-length", "5"), ("transfer-encoding", "chunked")]
                ),
                false
            )
            .unwrap(),
            BodyDecoder::Chunked { .. }
        ));
        assert!(
            BodyDecoder::for_response(&head(200, &[("transfer-encoding", "gzip")]), false).is_err()
        );
        assert!(
            BodyDecoder::for_response(
                &head(200, &[("content-length", "5"), ("content-length", "6")]),
                false
            )
            .is_err()
        );
    }
}
