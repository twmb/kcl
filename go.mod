module github.com/twmb/kcl

go 1.13

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/twmb/go-strftime v0.0.0-20190915101236-e74f7c4fe4fa
	github.com/twmb/kafka-go v0.0.0-20190412221104-4ef2a3ca30f4
)

replace github.com/twmb/kafka-go => ../kafka-go
