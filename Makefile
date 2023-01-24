.PHONY: update-deps
update-deps:
	go get -u ./...
	go mod tidy
