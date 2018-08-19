
GOCMD=go
GOBUILD=$(GOCMD) build
BINARYNAME=pipeline-demo

all: build

build:
	$(GOBUILD) -o $(BINARYNAME) cmd/main.go 