ARG GO_VERSION=1.22
ARG ALPINE_VERSION=3.19
ARG SERVICE=app

# First compile binary in a separate image
FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION} as builder

COPY . .

RUN go build -mod=vendor -o /go/bin/${SERVICE} .

FROM alpine:${ALPINE_VERSION}
ARG SERVICE=app

COPY --from=builder /go/bin/${SERVICE} /go/bin/${SERVICE}
ENV _executable_name=${SERVICE}
ENTRYPOINT /go/bin/${_executable_name}