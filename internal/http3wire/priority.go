package http3wire

import (
	"encoding/base64"
	"strings"
)

// Priority is the RFC 9218 HTTP scheduling signal. HTTP defaults differ from
// the transport API: incremental defaults to false.
type Priority struct {
	Urgency     int8
	Incremental bool
}

// ParsePriority parses the RFC 8941 Dictionary used by RFC 9218. Invalid
// dictionaries return defaults and false; unknown keys and valid members of
// unexpected types are ignored. The last duplicate key replaces earlier ones.
// Input size is bounded by the caller's HTTP field-section or control budget.
func ParsePriority(value string) (Priority, bool) {
	defaults := Priority{Urgency: 3}
	result := defaults
	p := priorityParser{text: value}
	p.spaces(false)
	for p.pos < len(p.text) {
		key, ok := p.key()
		if !ok {
			return defaults, false
		}
		kind, n := byte('b'), int64(1)
		if p.take('=') {
			if p.take('(') {
				kind = 'l'
				for {
					p.spaces(false)
					if p.take(')') {
						break
					}
					if _, _, ok = p.bare(); !ok || !p.parameters() {
						return defaults, false
					}
					if p.pos == len(p.text) || p.text[p.pos] != ' ' && p.text[p.pos] != ')' {
						return defaults, false
					}
				}
			} else {
				kind, n, ok = p.bare()
				if !ok {
					return defaults, false
				}
			}
		}
		if !p.parameters() {
			return defaults, false
		}
		switch key {
		case "u":
			result.Urgency = defaults.Urgency
			if kind == 'n' && n >= 0 && n <= 7 {
				result.Urgency = int8(n)
			}
		case "i":
			result.Incremental = false
			if kind == 'b' {
				result.Incremental = n == 1
			}
		}
		p.spaces(true)
		if p.pos == len(p.text) {
			return result, true
		}
		if !p.take(',') {
			return defaults, false
		}
		p.spaces(true)
		if p.pos == len(p.text) {
			return defaults, false
		}
	}
	return result, true
}

type priorityParser struct {
	text string
	pos  int
}

func (p *priorityParser) take(b byte) bool {
	if p.pos < len(p.text) && p.text[p.pos] == b {
		p.pos++
		return true
	}
	return false
}
func (p *priorityParser) spaces(tab bool) {
	for p.pos < len(p.text) && (p.text[p.pos] == ' ' || tab && p.text[p.pos] == '\t') {
		p.pos++
	}
}
func priorityKeyStart(b byte) bool { return b >= 'a' && b <= 'z' || b == '*' }
func priorityKeyRest(b byte) bool {
	return priorityKeyStart(b) || b >= '0' && b <= '9' || b == '_' || b == '-' || b == '.'
}
func (p *priorityParser) key() (string, bool) {
	start := p.pos
	if start == len(p.text) || !priorityKeyStart(p.text[start]) {
		return "", false
	}
	p.pos++
	for p.pos < len(p.text) && priorityKeyRest(p.text[p.pos]) {
		p.pos++
	}
	return p.text[start:p.pos], true
}
func (p *priorityParser) parameters() bool {
	for p.take(';') {
		p.spaces(false)
		if _, ok := p.key(); !ok {
			return false
		}
		if p.take('=') {
			if _, _, ok := p.bare(); !ok {
				return false
			}
		}
	}
	return true
}
func priorityAlpha(b byte) bool { return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' }
func priorityDigit(b byte) bool { return b >= '0' && b <= '9' }
func (p *priorityParser) bare() (byte, int64, bool) {
	if p.pos == len(p.text) {
		return 0, 0, false
	}
	b := p.text[p.pos]
	switch {
	case b == '?':
		p.pos++
		if p.take('0') {
			return 'b', 0, true
		}
		if p.take('1') {
			return 'b', 1, true
		}
		return 0, 0, false
	case b == '"':
		p.pos++
		for p.pos < len(p.text) {
			b = p.text[p.pos]
			p.pos++
			if b == '"' {
				return 's', 0, true
			}
			if b == '\\' {
				if p.pos == len(p.text) || p.text[p.pos] != '"' && p.text[p.pos] != '\\' {
					return 0, 0, false
				}
				p.pos++
			} else if b < 0x20 || b > 0x7e {
				return 0, 0, false
			}
		}
		return 0, 0, false
	case b == ':':
		p.pos++
		start := p.pos
		for p.pos < len(p.text) && p.text[p.pos] != ':' {
			b = p.text[p.pos]
			if !priorityAlpha(b) && !priorityDigit(b) && b != '+' && b != '/' && b != '=' {
				return 0, 0, false
			}
			p.pos++
		}
		end := p.pos
		if !p.take(':') {
			return 0, 0, false
		}
		encoded := p.text[start:end]
		encoding := base64.StdEncoding
		if !strings.Contains(encoded, "=") {
			encoding = base64.RawStdEncoding
		}
		_, err := encoding.DecodeString(encoded)
		return 'x', 0, err == nil
	case b == '-' || priorityDigit(b):
		negative := p.take('-')
		start := p.pos
		var number int64
		for p.pos < len(p.text) && priorityDigit(p.text[p.pos]) {
			if p.pos-start >= 15 {
				return 0, 0, false
			}
			number = number*10 + int64(p.text[p.pos]-'0')
			p.pos++
		}
		digits := p.pos - start
		if digits == 0 {
			return 0, 0, false
		}
		kind := byte('n')
		if p.take('.') {
			if digits > 12 {
				return 0, 0, false
			}
			kind = 'd'
			start = p.pos
			for p.pos < len(p.text) && priorityDigit(p.text[p.pos]) {
				p.pos++
			}
			if p.pos-start < 1 || p.pos-start > 3 {
				return 0, 0, false
			}
		}
		if negative {
			number = -number
		}
		return kind, number, true
	case priorityAlpha(b) || b == '*':
		p.pos++
		for p.pos < len(p.text) {
			b = p.text[p.pos]
			if !priorityAlpha(b) && !priorityDigit(b) && !strings.ContainsRune("!#$%&'*+-.^_`|~:/", rune(b)) {
				break
			}
			p.pos++
		}
		return 't', 0, true
	}
	return 0, 0, false
}
