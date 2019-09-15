module github.com/twmb/kcl

go 1.13

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.4-0.20181223182923-24fa6976df40 // indirect
	github.com/twmb/kgo v0.0.0-20190412221104-4ef2a3ca30f4
	golang.org/x/exp v0.0.0-20190411193353-0480eff6dd7c // indirect
)

replace github.com/twmb/kgo => ../kgo
