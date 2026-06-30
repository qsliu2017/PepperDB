//! `commands/copyfromparse.c`: COPY FROM input parsing.
//!
//! Splits the input into lines and each line into fields, honoring the text-format
//! backslash escapes (`\N` = NULL, `\t`, `\n`, octal/hex) and the CSV quoting /
//! escaping / embedded-newline rules, plus the NULL marker. Binary input is staged.
//!
//! The whole file is read into memory by `copyfrom.rs` and handed here as a byte
//! slice; `next_copy_line` returns the next logical line (CSV lines may span
//! physical newlines inside a quoted field). A field of `None` is a SQL NULL.

use crate::backend::commands::copy::CopyFormatOptions;

/// A cursor over the COPY input buffer that yields logical lines.
pub struct CopyReadState<'a> {
    data: &'a [u8],
    pos: usize,
    opts: &'a CopyFormatOptions,
}

impl<'a> CopyReadState<'a> {
    pub fn new(data: &'a [u8], opts: &'a CopyFormatOptions) -> Self {
        Self { data, pos: 0, opts }
    }

    /// True once the whole buffer has been consumed.
    pub fn at_eof(&self) -> bool {
        self.pos >= self.data.len()
    }

    /// PG `CopyReadLine`: read the next logical line as raw bytes (without the EOL).
    /// In CSV mode a quoted field may contain embedded newlines, so the scan tracks
    /// quote state. Returns `None` at end of input. A trailing `\.` line (text-mode
    /// end-of-data marker) terminates the input.
    pub fn read_line(&mut self) -> Option<Vec<u8>> {
        if self.at_eof() {
            return None;
        }
        let quotec = self.opts.quote as u8;
        let escapec = self.opts.escape as u8;
        let csv = self.opts.csv_mode;

        let start = self.pos;
        let mut in_quote = false;
        let mut line: Vec<u8> = Vec::new();
        while self.pos < self.data.len() {
            let c = self.data[self.pos];
            if csv {
                if in_quote {
                    if c == escapec
                        && self.pos + 1 < self.data.len()
                        && (self.data[self.pos + 1] == quotec || self.data[self.pos + 1] == escapec)
                    {
                        // Escaped quote/escape inside a quoted field: keep both bytes
                        // verbatim for the attribute parser to de-escape.
                        line.push(c);
                        line.push(self.data[self.pos + 1]);
                        self.pos += 2;
                        continue;
                    }
                    if c == quotec {
                        in_quote = false;
                    }
                    line.push(c);
                    self.pos += 1;
                    continue;
                } else if c == quotec {
                    in_quote = true;
                    line.push(c);
                    self.pos += 1;
                    continue;
                }
            }
            if c == b'\n' {
                self.pos += 1;
                // Strip a trailing CR (CRLF line ending).
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                break;
            }
            line.push(c);
            self.pos += 1;
        }
        let _ = start;

        // Text-mode end-of-data marker.
        if !csv && line == b"\\." {
            self.pos = self.data.len();
            return None;
        }
        Some(line)
    }
}

/// PG `CopyReadAttributesText`: split a text-format line into fields. Each field is
/// `Some(value)` (de-escaped) or `None` (matched the NULL marker).
pub fn read_attributes_text(line: &[u8], opts: &CopyFormatOptions) -> Vec<Option<String>> {
    let delimc = opts.delim as u8;
    let mut fields: Vec<Option<String>> = Vec::new();
    let mut i = 0;
    loop {
        let field_start = i;
        let mut out: Vec<u8> = Vec::new();
        let mut found_delim = false;
        while i < line.len() {
            let c = line[i];
            if c == delimc {
                found_delim = true;
                i += 1;
                break;
            }
            if c == b'\\' {
                i += 1;
                if i >= line.len() {
                    break;
                }
                let e = line[i];
                i += 1;
                match e {
                    b'0'..=b'7' => {
                        // Octal escape \013 (1-3 octal digits).
                        let mut val = u32::from(e - b'0');
                        for _ in 0..2 {
                            if i < line.len() && (b'0'..=b'7').contains(&line[i]) {
                                val = (val << 3) + u32::from(line[i] - b'0');
                                i += 1;
                            } else {
                                break;
                            }
                        }
                        out.push((val & 0xff) as u8);
                    }
                    b'x' => {
                        // Hex escape \x3F (1-2 hex digits).
                        if i < line.len() && line[i].is_ascii_hexdigit() {
                            let mut val = hex_val(line[i]);
                            i += 1;
                            if i < line.len() && line[i].is_ascii_hexdigit() {
                                val = (val << 4) + hex_val(line[i]);
                                i += 1;
                            }
                            out.push((val & 0xff) as u8);
                        } else {
                            out.push(b'x');
                        }
                    }
                    b'b' => out.push(0x08),
                    b'f' => out.push(0x0C),
                    b'n' => out.push(b'\n'),
                    b'r' => out.push(b'\r'),
                    b't' => out.push(b'\t'),
                    b'v' => out.push(0x0B),
                    other => out.push(other),
                }
                continue;
            }
            out.push(c);
            i += 1;
        }

        // Match the raw input (before de-escaping) against the NULL marker.
        let raw = &line[field_start..if found_delim { i - 1 } else { i }];
        if raw == opts.null_print.as_bytes() {
            fields.push(None);
        } else {
            fields.push(Some(String::from_utf8_lossy(&out).into_owned()));
        }

        if !found_delim {
            break;
        }
    }
    fields
}

/// PG `CopyReadAttributesCSV`: split a CSV line into fields, handling quoting and
/// escaping. An unquoted field equal to the NULL marker is `None`.
pub fn read_attributes_csv(line: &[u8], opts: &CopyFormatOptions) -> Vec<Option<String>> {
    let delimc = opts.delim as u8;
    let quotec = opts.quote as u8;
    let escapec = opts.escape as u8;
    let mut fields: Vec<Option<String>> = Vec::new();
    let mut i = 0;
    loop {
        let mut out: Vec<u8> = Vec::new();
        let mut saw_quote = false;
        let mut found_delim = false;

        loop {
            // Not in quote.
            let mut entered_quote = false;
            while i < line.len() {
                let c = line[i];
                if c == delimc {
                    found_delim = true;
                    i += 1;
                    break;
                }
                if c == quotec {
                    saw_quote = true;
                    entered_quote = true;
                    i += 1;
                    break;
                }
                out.push(c);
                i += 1;
            }
            if !entered_quote {
                break;
            }
            // In quote.
            loop {
                if i >= line.len() {
                    // Unterminated quote: tolerate (the line reader kept it whole).
                    break;
                }
                let c = line[i];
                if c == escapec
                    && i + 1 < line.len()
                    && (line[i + 1] == escapec || line[i + 1] == quotec)
                {
                    out.push(line[i + 1]);
                    i += 2;
                    continue;
                }
                if c == quotec {
                    i += 1;
                    break;
                }
                out.push(c);
                i += 1;
            }
        }

        if !saw_quote && out == opts.null_print.as_bytes() {
            fields.push(None);
        } else {
            fields.push(Some(String::from_utf8_lossy(&out).into_owned()));
        }

        if !found_delim {
            break;
        }
    }
    fields
}

fn hex_val(b: u8) -> u32 {
    match b {
        b'0'..=b'9' => u32::from(b - b'0'),
        b'a'..=b'f' => u32::from(b - b'a' + 10),
        b'A'..=b'F' => u32::from(b - b'A' + 10),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::commands::copy::{CopyFormatOptions, CopyHeaderChoice};

    fn text_opts() -> CopyFormatOptions {
        CopyFormatOptions {
            null_print: "\\N".into(),
            delim: '\t',
            ..CopyFormatOptions::default()
        }
    }

    fn csv_opts() -> CopyFormatOptions {
        CopyFormatOptions {
            csv_mode: true,
            null_print: String::new(),
            delim: ',',
            quote: '"',
            escape: '"',
            header_line: CopyHeaderChoice::False,
            ..CopyFormatOptions::default()
        }
    }

    #[test]
    fn text_fields_and_null_and_escapes() {
        let opts = text_opts();
        let f = read_attributes_text(b"1\thello\t\\N", &opts);
        assert_eq!(f, vec![Some("1".into()), Some("hello".into()), None]);

        // Escapes: \t inside a field, \\ literal backslash, \n newline.
        let f = read_attributes_text(b"a\\tb\t\\\\\tx\\ny", &opts);
        assert_eq!(f, vec![Some("a\tb".into()), Some("\\".into()), Some("x\ny".into())]);
    }

    #[test]
    fn csv_fields_quoting_and_embedded() {
        let opts = csv_opts();
        // A quoted field containing a comma and a doubled quote.
        let line = br#"1,"a,b","she said ""hi""""#;
        let f = read_attributes_csv(line, &opts);
        assert_eq!(
            f,
            vec![Some("1".into()), Some("a,b".into()), Some("she said \"hi\"".into())]
        );

        // Unquoted empty field is NULL (default CSV null marker).
        let f = read_attributes_csv(b"1,,3", &opts);
        assert_eq!(f, vec![Some("1".into()), None, Some("3".into())]);

        // Quoted empty field is the empty string, NOT NULL.
        let f = read_attributes_csv(br#"1,"",3"#, &opts);
        assert_eq!(f, vec![Some("1".into()), Some(String::new()), Some("3".into())]);
    }

    #[test]
    fn csv_line_spans_embedded_newline() {
        let opts = csv_opts();
        let mut reader = CopyReadState::new(b"\"line1\nline2\",x\nnext,y\n", &opts);
        let l1 = reader.read_line().unwrap();
        let f1 = read_attributes_csv(&l1, &opts);
        assert_eq!(f1, vec![Some("line1\nline2".into()), Some("x".into())]);
        let l2 = reader.read_line().unwrap();
        let f2 = read_attributes_csv(&l2, &opts);
        assert_eq!(f2, vec![Some("next".into()), Some("y".into())]);
        assert!(reader.read_line().is_none());
    }

    #[test]
    fn custom_delimiter_and_null_marker() {
        let opts = CopyFormatOptions {
            null_print: "NULL".into(),
            delim: '|',
            ..CopyFormatOptions::default()
        };
        let f = read_attributes_text(b"a|NULL|c", &opts);
        assert_eq!(f, vec![Some("a".into()), None, Some("c".into())]);
    }
}
