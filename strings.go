package tsdbclient

import "strings"

var (
	escaper   = strings.NewReplacer(`,`, `\,`, `"`, `\"`, ` `, `\ `, `=`, `\=`)
	unescaper = strings.NewReplacer(`\,`, `,`, `\"`, `"`, `\ `, ` `, `\=`, `=`)
)

// UnescapeString returns unescaped version of in.
func UnescapeString(in string) string {
	if strings.IndexByte(in, '\\') == -1 {
		return in
	}
	return unescaper.Replace(in)
}

// String0 returns the escaped version of in.
func String0(in string) string {
	return escaper.Replace(in)
}
