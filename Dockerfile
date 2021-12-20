FROM golang:1.17.3

RUN apt-get update && apt-get install -y ca-certificates

WORKDIR /workspace

COPY go.* ./

RUN go mod download

COPY *.go ./

COPY ./pkg ./pkg

COPY ./cmd/auth ./cmd/auth

RUN CGO_ENABLED=0 go build -ldflags '-s' -v -o /bin/auth ./cmd/auth

FROM scratch

COPY --from=0 /etc/passwd /etc/passwd

COPY --from=0 /bin/auth /bin/auth

COPY --from=0 /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

USER nobody

CMD ["/bin/auth"]
