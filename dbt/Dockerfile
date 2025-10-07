# # dbt/Dockerfile
# FROM grmv/dbt-clickhouse:1.8.6

# # Alpine package manager
# RUN apk add --no-cache git


FROM grmv/dbt-clickhouse:1.8.6

# Temporarily switch repositories to HTTP to fetch certs
RUN sed -i 's|https://dl-cdn.alpinelinux.org|http://dl-cdn.alpinelinux.org|g' /etc/apk/repositories \
    && apk update \
    && apk add --no-cache ca-certificates git \
    && update-ca-certificates \
    # Restore HTTPS after certs are fixed
    && sed -i 's|http://dl-cdn.alpinelinux.org|https://dl-cdn.alpinelinux.org|g' /etc/apk/repositories



