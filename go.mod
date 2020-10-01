module github.com/ceymard/dmut

go 1.15

replace github.com/alecthomas/participle => /home/chris/projects/participle

require (
	github.com/alecthomas/kong v0.2.11
	github.com/alecthomas/participle v0.6.1-0.20200905115227-c1de023f7c13
	github.com/jackc/pgx/v4 v4.9.0
	github.com/k0kubun/colorstring v0.0.0-20150214042306-9440f1994b88 // indirect
	github.com/k0kubun/pp v3.0.1+incompatible
	github.com/lib/pq v1.8.0
	github.com/logrusorgru/aurora v2.0.3+incompatible
	github.com/mattn/go-colorable v0.1.7 // indirect
	github.com/pkg/errors v0.9.1
	golang.org/x/crypto v0.0.0-20200930160638-afb6bcd081ae // indirect
)
