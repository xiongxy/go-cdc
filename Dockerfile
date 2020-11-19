# 第一阶段
FROM golang:1.14 AS stage_one
ENV GO111MODULE=on \
    GOPROXY=https://goproxy.cn
WORKDIR /app
COPY . .
RUN mkdir dist && go build -o /app/dist/cdc-distribute && cp /app/.env /app/dist/

# 第二阶段
FROM busybox:glibc
WORKDIR /dist
COPY --from=stage_one /app/dist/ .
EXPOSE 5000
ENTRYPOINT ["./cdc-distribute"]
