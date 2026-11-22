package http3wire

import "testing"

func TestParsePriority(t *testing.T) {
	tests := []struct {
		input    string
		u        int8
		i, valid bool
	}{
		{"", 3, false, true}, {"u=0, i", 0, true, true}, {"u=7, i=?0", 7, false, true},
		{"u=8, i=?1", 3, true, true}, {"u=-1", 3, false, true}, {"u=-0", 0, false, true},
		{"u=2.0, i=1", 3, false, true}, {"u=2, u=4", 4, false, true}, {"i, i=?0", 3, false, true},
		{"u=1, u=wrong", 3, false, true}, {"i, i=wrong", 3, false, true},
		{"u=5;note=token, i;flag", 5, true, true},
		{"x=(1 \"two\";a :YWJj:);p=?0, u=1", 1, false, true},
		{"u=(0), i=(?1)", 3, false, true},
		{"x=:YQ:, u=2", 2, false, true}, {"x=:YR==:, u=2", 2, false, true},
		{"x=abc:/DEF, y=999999999999999, z=-999999999999.999, i", 3, true, true},
		{" u=1 \t,\t i\t", 1, true, true},
		{"u=1,", 3, false, false}, {"u=1,,i", 3, false, false}, {"U=1", 3, false, false},
		{"u =1", 3, false, false}, {"u= 1", 3, false, false}, {"u=1;", 3, false, false},
		{"u=1, x=\"bad\\q\"", 3, false, false}, {"x=\"bad\n\"", 3, false, false},
		{"x=1000000000000000", 3, false, false}, {"x=1000000000000.0", 3, false, false},
		{"x=1.0000", 3, false, false}, {"x=1.", 3, false, false}, {"x=-", 3, false, false},
		{"x=((1))", 3, false, false}, {"x=(1,2)", 3, false, false}, {"x=(1\t2)", 3, false, false},
		{"x=:a:", 3, false, false}, {"x=:YQ===:", 3, false, false}, {"x=:YQ\n==:", 3, false, false},
		{"i=?2", 3, false, false}, {"u=1, x=\"é\"", 3, false, false}, {"\tu=1", 3, false, false},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, valid := ParsePriority(test.input)
			if got.Urgency != test.u || got.Incremental != test.i || valid != test.valid {
				t.Fatal(got, valid)
			}
		})
	}
}

func FuzzParsePriority(f *testing.F) {
	for _, seed := range []string{"u=0, i", "x=(1 \"two\";a), u=5", "x=:YQ==:", "u=2, u=9", "u=0, x=((1))"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 8192 {
			return
		}
		priority, valid := ParsePriority(input)
		if priority.Urgency < 0 || priority.Urgency > 7 {
			t.Fatal(priority)
		}
		if !valid && priority != (Priority{Urgency: 3}) {
			t.Fatal("partial malformed result", priority)
		}
	})
}
