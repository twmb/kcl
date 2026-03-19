module github.com/twmb/kcl

go 1.25

require (
	github.com/BurntSushi/toml v1.5.0
	github.com/aws/aws-sdk-go-v2/config v1.31.5
	github.com/jhump/protoreflect v1.17.0
	github.com/spf13/cobra v1.9.1
	github.com/spf13/pflag v1.0.7
	github.com/twmb/franz-go v1.20.6
	github.com/twmb/franz-go/pkg/kadm v1.17.1
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20260315151839-72fe24297b24
	github.com/twmb/franz-go/pkg/kmsg v1.12.0
	golang.org/x/crypto v0.48.0
	google.golang.org/protobuf v1.36.8
)

require (
	github.com/aws/aws-sdk-go-v2 v1.38.2 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.18.9 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.29.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.34.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.38.1 // indirect
	github.com/aws/smithy-go v1.23.0 // indirect
	github.com/bufbuild/protocompile v0.14.1 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/klauspost/compress v1.18.4 // indirect
	github.com/pierrec/lz4/v4 v4.1.25 // indirect
	golang.org/x/sync v0.16.0 // indirect
)

replace (
	github.com/twmb/franz-go => ../franz-go
	github.com/twmb/franz-go/pkg/kadm => ../franz-go/pkg/kadm
	github.com/twmb/franz-go/pkg/kfake => ../franz-go/pkg/kfake
	github.com/twmb/franz-go/pkg/kmsg => ../franz-go/pkg/kmsg
)
