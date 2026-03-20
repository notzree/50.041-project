FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /node .

FROM alpine:latest
COPY --from=builder /node /node
ENTRYPOINT ["/node"]
