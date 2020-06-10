FROM golang:1.14 as builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY ./src .
RUN go build -o server .
EXPOSE 8080
CMD ["/app/server"]
