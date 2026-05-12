FROM golang:1.22-alpine3.21 AS builder
ARG VERSION
RUN apk --no-cache update && apk --no-cache add gcc musl-dev git make bash
WORKDIR /project
COPY . .
RUN go build -ldflags "-X main.version=${VERSION}" -tags musl -v -o bin/scheduler ./cmd/kafka

FROM alpine:3.21
RUN apk --no-cache update
WORKDIR /project
COPY --from=builder /project/bin/scheduler scheduler
USER 1001
CMD ["./scheduler"]
