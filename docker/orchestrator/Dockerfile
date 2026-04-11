FROM python:3.12-slim

ARG BRUIN_VERSION=0.11.464
ARG TARGETARCH

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/workspace \
    UV_CACHE_DIR=/workspace/.uv-cache \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/workspace/.venv \
    VIRTUAL_ENV=/workspace/.venv \
    PATH="/workspace/.venv/bin:/root/.local/bin:${PATH}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates curl git \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh \
    && mv /root/.local/bin/uv /usr/local/bin/uv

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

WORKDIR /workspace

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY dbt_project.yml profiles.yml .bruin.yml Makefile ./
COPY bruin ./bruin
COPY ingest ./ingest
COPY macros ./macros
COPY models ./models
COPY tests ./tests
COPY warehouse ./warehouse
COPY scripts ./scripts

RUN chmod +x /workspace/scripts/*.sh \
    && printf '%s\n' \
      'if [ -f /workspace/scripts/google_auth_env.sh ]; then' \
      '  . /workspace/scripts/google_auth_env.sh' \
      '  configure_google_auth --quiet >/dev/null 2>&1 || true' \
      'fi' >> /root/.bashrc \
    && printf '%s\n' \
      'if [ -f ~/.bashrc ]; then' \
      '  . ~/.bashrc' \
      'fi' > /root/.bash_profile \
    && mkdir -p /workspace/data /workspace/logs /workspace/target /workspace/dbt_packages /workspace/.uv-cache

ENTRYPOINT ["/workspace/scripts/container_entrypoint.sh"]
CMD ["tail", "-f", "/dev/null"]
