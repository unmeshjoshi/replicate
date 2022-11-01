FROM openjdk:17-alpine
ARG GOLANG_VERSION=1.16.5
ARG PROTOC_VERSION=3.16.0
RUN apk add wget

#we need the go version installed from apk to bootstrap the custom version built from source
RUN apk update && apk add go gcc bash musl-dev openssl-dev ca-certificates && update-ca-certificates

RUN wget https://dl.google.com/go/go$GOLANG_VERSION.src.tar.gz && tar -C /usr/local -xzf go$GOLANG_VERSION.src.tar.gz

RUN cd /usr/local/go/src && ./make.bash

ENV PATH=$PATH:/usr/local/go/bin

RUN rm go$GOLANG_VERSION.src.tar.gz

#we delete the apk installed version to avoid conflict
RUN apk del go

RUN go version
ARG PB_REL="https://github.com/protocolbuffers/protobuf/releases"
RUN apk update && apk add --no-cache make
RUN apk add --no-cache curl
RUN apk add --no-cache protoc
RUN apk add --no-cache git
#RUN echo "$PB_REL/download/v$PROTOC_VERSION/protoc-$PROTOC_VERSION-linux-x86_64.zip"
#RUN curl -LO $PB_REL/download/v$PROTOC_VERSION/protoc-$PROTOC_VERSION-linux-x86_64.zip
#RUN unzip protoc-$PROTOC_VERSION-linux-x86_64.zip -d /local
#RUN go version
#RUN GO111MODULE=off go get github.com/gogo/protobuf/proto
#RUN go get github.com/gogo/protobuf/protoc-gen-gogo
#ENV PATH="$PATH:$GOPATH/bin:/local/bin"
VOLUME /work
WORKDIR /work
