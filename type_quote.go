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
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('\'')
	for i := 0; i < len(s); i++ {
		if e, ok := quoteEscapes[s[i]]; ok {
			b.WriteByte('\\')
			b.WriteByte(e)
			continue
		}
		b.WriteByte(s[i])
	}
	b.WriteByte('\'')
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
