package mutations

import (
	"os"
	"strings"
	"testing"
	"unicode"

	lexer "github.com/alecthomas/participle/v2/lexer"
	"github.com/goccy/go-yaml"
	au "github.com/logrusorgru/aurora"
)

type autoDownTest struct {
	Up   string `yaml:"up"`
	Down string `yaml:"down"`
}

func TestAutoDowner(t *testing.T) {

	var tests []autoDownTest
	test_file, err := os.Open("test/auto-down-tests.yml")
	if err != nil {
		t.Fatalf("error opening auto-down-tests.yml: %v", err)
	}
	defer test_file.Close()

	if err := yaml.NewDecoder(test_file).Decode(&tests); err != nil {
		t.Fatalf("error decoding auto-down-tests.yml: %v", err)
	}

	id_token := SqlLexer.Symbols()["Id"]
	for _, tt := range tests {
		t.Run(tt.Up, func(t *testing.T) {
			got, err := AutoDowner.ParseAndGetDefault(tt.Up)
			if err != nil {
				t.Fatalf("parse error: %v - %s", red(err.Error()), tt.Up)
			}
			if !compareTokens(t, id_token, got, tt.Down) {
				displayMismatch(t, strings.ToLower(tt.Down), strings.ToLower(got))
			}
		})
	}
}

func compareIdToken(cmp lexer.Token, got lexer.Token) bool {
	if len(cmp.Value) > 0 && cmp.Value[0] == '"' || len(got.Value) > 0 && got.Value[0] == '"' {
		return cmp.Value == got.Value
	}
	return strings.EqualFold(cmp.Value, got.Value)
}

func green(s string) string {
	return au.BrightGreen(s).String()
}

func red(s string) string {
	return au.BrightRed(s).String()
}

// lcsAlignment returns which runes in wants and got belong to a longest common subsequence.
// wantInLCS[i] is true if wants[i] is matched in the LCS; gotInLCS[j] is true if got[j] is matched.
func lcsAlignment(wants, got []lexer.Token) (wantInLCS []bool, gotInLCS []bool, has_diffs bool) {
	m, n := len(wants), len(got)
	// dp[i][j] = length of LCS of wants[:i] and got[:j]
	dp := make([][]int, m+1)
	for i := range dp {
		dp[i] = make([]int, n+1)
	}
	for i := 1; i <= m; i++ {
		for j := 1; j <= n; j++ {
			if compareIdToken(wants[i-1], got[j-1]) {
				dp[i][j] = dp[i-1][j-1] + 1
			} else {
				if dp[i-1][j] > dp[i][j-1] {
					dp[i][j] = dp[i-1][j]
				} else {
					dp[i][j] = dp[i][j-1]
				}
			}
		}
	}
	wantInLCS = make([]bool, m)
	gotInLCS = make([]bool, n)
	i, j := m, n
	for i > 0 && j > 0 {
		if compareIdToken(wants[i-1], got[j-1]) {
			has_diffs = true
			wantInLCS[i-1] = true
			gotInLCS[j-1] = true
			i--
			j--
		} else if dp[i-1][j] >= dp[i][j-1] {
			i--
		} else {
			j--
		}
	}
	return wantInLCS, gotInLCS, has_diffs
}

func printToken(src string, tk lexer.Token) string {
	str := ""
	if tk.Pos.Offset > 0 && unicode.IsSpace(rune(src[tk.Pos.Offset-1])) {
		str += " "
	}
	str += tk.Value
	return str
}

// With the green(string) string and red(string) string functions, display the mismatch in a pretty way
func displayMismatch(t *testing.T, wants string, got string) {
	wantsTokens, err := split(wants)
	if err != nil {
		t.Errorf("error splitting cmp: %v", err)
		return
	}
	gotTokens, err := split(got)
	if err != nil {
		t.Errorf("error splitting got: %v", err)
		return
	}

	wantInLCS, gotInLCS, has_diffs := lcsAlignment(wantsTokens, gotTokens)
	if has_diffs {
		var wantOut, gotOut strings.Builder
		for i, tk := range wantsTokens {
			if wantInLCS[i] {
				wantOut.WriteString(printToken(wants, tk))
			} else {
				wantOut.WriteString(green(printToken(wants, tk)))
			}
		}
		for j, tk := range gotTokens {
			if gotInLCS[j] {
				gotOut.WriteString(printToken(got, tk))
			} else {
				gotOut.WriteString(red(printToken(got, tk)))
			}
		}
		t.Errorf("want: %s", wantOut.String())
		t.Errorf("got:  %s", gotOut.String())
	}
}

func compareTokens(t *testing.T, id_token lexer.TokenType, cmp string, got string) bool {
	cmpTokens, err := split(cmp)
	if err != nil {
		return false
	}
	gotTokens, err := split(got)
	if err != nil {
		return false
	}
	if len(cmpTokens) != len(gotTokens) {
		return false
	}
	for i := range cmpTokens {

		if cmpTokens[i].Type == id_token {
			// ids are case insensitive
			if !compareIdToken(cmpTokens[i], gotTokens[i]) {

				return false
			}
		} else {
			if cmpTokens[i].Value != gotTokens[i].Value {

				return false
			}
		}
	}
	return true
}
