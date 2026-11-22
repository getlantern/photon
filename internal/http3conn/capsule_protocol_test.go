package http3conn

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/getlantern/photon/internal/qpack"
)

func TestCapsuleProtocol(t *testing.T) {
	valid := []string{`?1`, `  ?1  `, `?1;a;b=?0;a=?1`, `?1; *x_9.-=Token:/!#$%&'*+-.^_` + "`" + `|~`, `?1;x="escaped\"quote\\"`, `?1;x=999999999999999;y=-999999999999999`, `?1;x=999999999999.999;y=-0.001`, `?1;x=::;y=:aGVsbG8=:;z=:aGVsbG8:`, `?1;x=:iZ==:`, `?1;x=@-999999999999999`, `?1;x=%"%f0%9f%98%80%00%22%25"`, `?1;x=%"literal\backslash"`}
	invalid := []string{``, `?0`, `true`, `1`, `(?1)`, `?1, ?1`, `?11`, "\t?1", "?1\t", `?1 ;a`, `?1;`, `?1;A`, `?1;a =1`, `?1;a= 1`, `?1;a=?2`, `?1;a=`, `?1;a=+1`, `?1;a=1000000000000000`, `?1;a=1000000000000.0`, `?1;a=1.`, `?1;a=.1`, `?1;a=1.0000`, `?1;a=@1.0`, `?1;a="bad\n"`, "?1;a=\"é\"", `?1;a=:a:`, `?1;a=:ab===:`, `?1;a=:abcd=:`, `?1;a=:ab_c:`, `?1;a=:a b:`, `?1;a=:ab=c:`, `?1;a=%"%C3%a9"`, `?1;a=%"%c3"`, `?1;a=%"%c0%80"`, `?1;a=%"%ed%a0%80"`, `?1;a=%"%f4%90%80%80"`, `?1;a=%"%ff"`, `?1;a=%"é"`, `?1;a=%"%0"`}
	for _, group := range []struct {
		values []string
		want   bool
	}{{valid, true}, {invalid, false}} {
		for _, value := range group.values {
			t.Run(value, func(t *testing.T) {
				if got := CapsuleProtocol(http.Header{"cApSuLe-PrOtOcOl": {value}}); got != group.want {
					t.Fatalf("got %v, want %v", got, group.want)
				}
			})
		}
	}
	for _, h := range []http.Header{nil, {"Capsule-Protocol": {"?1", "?1"}}, {"Capsule-Protocol": {"?1"}, "capsule-protocol": {"?0"}}} {
		if CapsuleProtocol(h) {
			t.Fatal("accepted absent or repeated header", h)
		}
	}
}

func TestCapsuleMessagePolicy(t *testing.T) {
	for _, status := range []string{"103", "200", "204", "205", "206", "299", "403"} {
		for _, field := range []string{"", "capsule-protocol", "content-length", "content-type"} {
			t.Run(status+"/"+field, func(t *testing.T) {
				m, err := NewMessage(MessageConfig{Kind: ResponseFields, RequestMethod: "CONNECT", RequestProtocol: "capsule-test", Capsules: true})
				if err != nil {
					t.Fatal(err)
				}
				fields := responseFields(status)
				if field != "" {
					value := "?1"
					if field == "content-length" {
						value = "0"
					}
					fields = append(fields, fs(field, value))
				}
				_, _, err = m.Headers(fields)
				success := status[0] == '2'
				wantError := success && (status == "204" || status == "205" || status == "206" || field == "content-length" || field == "content-type") || !success && field == "capsule-protocol"
				wantError = wantError || status == "103" && field == "content-length"
				if (err != nil) != wantError {
					t.Fatalf("error %v, want error %v", err, wantError)
				}
				if err != nil {
					var message *MessageError
					if !errors.As(err, &message) || m.Data(0) != err {
						t.Fatalf("non-sticky or wrong scope: %v", err)
					}
				}
			})
		}
	}
	for _, extended := range []bool{false, true} {
		for _, content := range []bool{false, true} {
			m, _ := NewMessage(MessageConfig{Kind: RequestFields, ExtendedConnect: true, Capsules: true})
			fields := []qpack.DecodedField{fs(":method", "CONNECT"), fs(":authority", "example.org:443")}
			if extended {
				fields = append(fields, fs(":scheme", "https"), fs(":path", "/"), fs(":protocol", "capsule-test"))
			}
			if content {
				fields = append(fields, fs("content-type", "application/octet-stream"))
			}
			_, _, err := m.Headers(fields)
			if (err != nil) != (!extended || content) {
				t.Fatalf("extended=%v content=%v: %v", extended, content, err)
			}
		}
	}
	// The header alone must not activate capsule restrictions on an ordinary response.
	m := newResponse(t)
	if _, _, err := m.Headers(append(responseFields("200"), fs("capsule-protocol", "?1"), fs("content-length", "3"))); err != nil {
		t.Fatal(err)
	}
	if err := m.Data(3); err != nil {
		t.Fatal(err)
	}
	if err := m.Finish(); err != nil {
		t.Fatal(err)
	}
}

func FuzzCapsuleProtocol(f *testing.F) {
	for _, s := range []string{"", "?1", "?1;a=%\"%f0%9f%98%80\"", "?1;x=:iZ==:", "?1;a=999999999999999", "\xff\xc0\x80"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		_ = CapsuleProtocol(http.Header{"Capsule-Protocol": {s}})
		// Fully percent-encoded display strings admit exactly valid UTF-8 bytes.
		var encoded strings.Builder
		encoded.WriteString(`?1;x=%"`)
		for i := 0; i < len(s); i++ {
			const hex = "0123456789abcdef"
			encoded.WriteByte('%')
			encoded.WriteByte(hex[s[i]>>4])
			encoded.WriteByte(hex[s[i]&15])
		}
		encoded.WriteByte('"')
		if capsuleBoolean(encoded.String()) != utf8.ValidString(s) {
			t.Fatalf("UTF-8 disagreement for %x", s)
		}
	})
}
