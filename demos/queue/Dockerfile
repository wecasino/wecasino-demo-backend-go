FROM alpine:3.17.3

WORKDIR /app
ENV ADDR=:80
COPY main /app/main

EXPOSE 80

ENTRYPOINT ["/app/main"]
