# Copyright European Organization for Nuclear Research (CERN) 2017
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Yuyi Guo, <yuyi@fnal.gov>, 2021


FROM golang:latest as go-builder

# build procedure
ENV WDIR=/data
WORKDIR $WDIR
RUN mkdir -p /data/{stompserver,gopath, etc} && mkdir /build
ENV GOPATH=/data/gopath
ARG CGO_ENABLED=0
# get go libraries
RUN go get github.com/lestrrat-go/file-rotatelogs
RUN go get github.com/vkuznet/lb-stomp
RUN go get github.com/go-stomp/stomp
# build Rucio tracer
WORKDIR $WDIR/etc
RUN curl -ksLO https://raw.githubusercontent.com/yuyiguo/CMSRucio/RucioTraces/etc/ruciositemap.json
WORKDIR $WDIR/stompserver
RUN curl -ksLO https://raw.githubusercontent.com/yuyiguo/CMSRucio/RucioTraces/stompserver/stompserver.go
RUN go mod init github.com/yuyiguo/CMSRucio/RucioTraces/stompserver && go mod tidy && \
    go build -o /build/RucioTracer -ldflags="-s -w -extldflags -static" /data/stompserver/stompserver.go
FROM alpine
RUN mkdir -p /data
COPY --from=go-builder /build/RucioTracer /data/
