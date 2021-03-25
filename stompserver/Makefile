VERSION=`git rev-parse --short HEAD`
flags=-ldflags="-s -w -X main.version=${VERSION}"

all: build

build:
	go clean; rm -rf pkg; go build -o stompserver ${flags}

build_debug:
	go clean; rm -rf pkg; go build -o stompserver ${flags} -gcflags="-m -m"

build_all: build_osx build_linux build

build_osx:
	go clean; rm -rf pkg stompserver_osx; GOOS=darwin go build -o stompserver ${flags}

build_linux:
	go clean; rm -rf pkg stompserver_linux; GOOS=linux go build -o stompserver ${flags}

build_power8:
	go clean; rm -rf pkg stompserver_power8; GOARCH=ppc64le GOOS=linux go build -o stompserver ${flags}

build_arm64:
	go clean; rm -rf pkg stompserver_arm64; GOARCH=arm64 GOOS=linux go build -o stompserver ${flags}

build_windows:
	go clean; rm -rf pkg stompserver.exe; GOARCH=amd64 GOOS=windows go build -o stompserver ${flags}

install:
	go install

clean:
	go clean; rm -rf pkg

test : test1

test1:
	go test
