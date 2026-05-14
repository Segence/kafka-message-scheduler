FROM segence/go-build:0.4.0-kafka AS builder

ARG VERSION

WORKDIR /project
COPY . .
# RUN go build -ldflags "-extldflags -static -X main.version=${VERSION}" -tags musl -v -o bin/scheduler ./cmd/kafka


RUN go mod download
RUN go mod tidy

RUN go build -tags musl --ldflags "-extldflags -static -X main.version=${VERSION}" -o main ./cmd/kafka

FROM alpine:3.21
RUN apk --no-cache update
WORKDIR /project
COPY --from=builder /project/main scheduler
USER 1001
CMD ["./scheduler"]
