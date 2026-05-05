ARG BASE_IMAGE=capstone-pipeline:local
FROM ${BASE_IMAGE}

ARG BRUIN_VERSION=0.11.464
ARG TARGETARCH

RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) BRUIN_ARCH="x86_64"; BRUIN_SHA256="a8ddbe5546c8ff89d4343b612b5eb3c248738670ede8a3866193ac9432960e25" ;; \
      arm64) BRUIN_ARCH="arm64"; BRUIN_SHA256="5a8a6ae5211f7fbed5215386ca3f224316d07a7d3d23443d3f6d4ed06d3739b2" ;; \
      *) echo "Unsupported TARGETARCH: ${TARGETARCH}"; exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/bruin.tar.gz "https://github.com/bruin-data/bruin/releases/download/v${BRUIN_VERSION}/bruin_Linux_${BRUIN_ARCH}.tar.gz"; \
    echo "${BRUIN_SHA256}  /tmp/bruin.tar.gz" | sha256sum -c -; \
    tar -xzf /tmp/bruin.tar.gz -C /usr/local/bin bruin; \
    chmod +x /usr/local/bin/bruin; \
    rm -f /tmp/bruin.tar.gz

COPY bruin ./bruin
