# Docker image for core-svc cronjob
FROM alpine:3.18.2
RUN apk add --no-cache ca-certificates=20230506-r0 curl=8.1.2-r0 bash=5.2.15-r5 && rm -rf /var/cache/apk/*
RUN curl -LO "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl" &&\
    chmod +x ./kubectl &&\
    mv ./kubectl /usr/bin/kubectl

RUN mkdir /code
WORKDIR /code
COPY cleanup.sh /code/

ENTRYPOINT ["/bin/bash", "/code/cleanup.sh"]
