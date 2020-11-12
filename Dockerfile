FROM golang:1.14

ENV GO111MODULE=on \
    GOPROXY=https://goproxy.cn

WORKDIR /app

COPY . .

RUN go build .

ENTRYPOINT ["./cdc-distribute"]