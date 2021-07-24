FROM golang:1.15.6-alpine
WORKDIR /
COPY ./peers.txt /
COPY . /
 RUN go build -o dht-server ./src/server/dht-server.go
ENTRYPOINT ["go", "run", "./src/server/dht-server.go"]
