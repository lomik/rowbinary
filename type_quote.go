package rowbinary

import (
	"fmt"
	"strings"
)

// Escapes ClickHouse uses for single-quoted strings in type names, as printed
// by toTypeName (checked against ClickHouse 25.9).
var quoteEscapes = map[byte]byte{
	'\\': '\\',
	'\'': '\'',
	'\n': 'n',
	'\t': 't',
	'\r': 'r',
	'\b': 'b',
	'\f': 'f',
	0:    '0',
}

var unquoteEscapes = map[byte]byte{
	'\\': '\\',
	'\'': '\'',
	'n':  '\n',
	't':  '\t',
	'r':  '\r',
	'b':  '\b',
	'f':  '\f',
	'0':  0,
}

// quote returns s as a single-quoted ClickHouse string literal.
func quote(s string) string {
	return quoteWith(s, '\'')
}

// QuoteIdentifier returns a ClickHouse identifier in backquotes, escaped so
// that any name is safe to put into SQL, for example as the table argument of
// Insert. Several parts are joined with dots: QuoteIdentifier("db", "t")
// returns `db`.`t`, while QuoteIdentifier("a.b") quotes one name containing a
// dot.
func QuoteIdentifier(parts ...string) string {
	quoted := make([]string, len(parts))
	for i, p := range parts {
		quoted[i] = quoteWith(p, '`')
	}
	return strings.Join(quoted, ".")
}

// quoteWith quotes s with q, escaping q, backslash and control characters the
// way ClickHouse does for both string literals and backquoted identifiers.
func quoteWith(s string, q byte) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte(q)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == q {
			b.WriteByte('\\')
			b.WriteByte(c)
			continue
		}
		if e, ok := quoteEscapes[c]; ok && c != '\'' {
			b.WriteByte('\\')
			b.WriteByte(e)
			continue
		}
		b.WriteByte(c)
	}
	b.WriteByte(q)
	return b.String()
}

// scanQuoted reads the single-quoted literal starting at s[start] (which must
// be a single quote) and returns its unescaped value and the index just past the closing
// quote. An unknown escape such as \x yields the character itself.
func scanQuoted(s string, start int) (string, int, error) {
	if start >= len(s) || s[start] != '\'' {
		return "", 0, fmt.Errorf("expected a quoted string in %q", s)
	}
	var b strings.Builder
	for i := start + 1; i < len(s); i++ {
		switch s[i] {
		case '\'':
			return b.String(), i + 1, nil
		case '\\':
			i++
			if i >= len(s) {
				return "", 0, fmt.Errorf("unterminated quote in %q", s)
			}
			if c, ok := unquoteEscapes[s[i]]; ok {
				b.WriteByte(c)
			} else {
				b.WriteByte(s[i])
			}
		default:
			b.WriteByte(s[i])
		}
	}
	return "", 0, fmt.Errorf("unterminated quote in %q", s)
}

// unquote returns the value of s if it is exactly one quoted literal, and the
// trimmed s otherwise (for example an unquoted time zone name).
func unquote(s string) string {
	s = strings.TrimSpace(s)
	if v, end, err := scanQuoted(s, 0); err == nil && end == len(s) {
		return v
	}
	return s
}

// skipQuoted returns the index just past the quoted literal starting at
// s[start], or len(s) if it is unterminated (callers validate separately).
func skipQuoted(s string, start int) int {
	if _, end, err := scanQuoted(s, start); err == nil {
		return end
	}
	return len(s)
}

// parseEnumElement parses one `'label' = value` element of an Enum type.
func parseEnumElement(s string) (label, value string, err error) {
	s = strings.TrimSpace(s)
	label, end, err := scanQuoted(s, 0)
	if err != nil {
		return "", "", fmt.Errorf("can't parse enum element %q: %w", s, err)
	}
	rest := strings.TrimSpace(s[end:])
	if !strings.HasPrefix(rest, "=") {
		return "", "", fmt.Errorf("can't parse enum element %q: expected '='", s)
	}
	return label, strings.TrimSpace(rest[1:]), nil
}

// quoteIdentifierIfNeeded returns name unchanged if it is a plain identifier
// ([A-Za-z_][A-Za-z0-9_]*), and backquoted otherwise.
func quoteIdentifierIfNeeded(name string) string {
	for i := 0; i < len(name); i++ {
		c := name[i]
		letter := c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		if !letter && (i == 0 || c < '0' || c > '9') {
			return QuoteIdentifier(name)
		}
	}
	if name == "" {
		return QuoteIdentifier(name)
	}
	return name
}
