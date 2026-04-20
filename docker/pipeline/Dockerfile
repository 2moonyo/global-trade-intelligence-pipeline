FROM python:3.12-slim

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

WORKDIR /workspace

RUN printf '%s\n' \
      'export VIRTUAL_ENV="${VIRTUAL_ENV:-/workspace/.venv}"' \
      'export UV_PROJECT_ENVIRONMENT="${UV_PROJECT_ENVIRONMENT:-/workspace/.venv}"' \
      'export UV_CACHE_DIR="${UV_CACHE_DIR:-/workspace/.uv-cache}"' \
      'case ":${PATH}:" in' \
      '  *":${VIRTUAL_ENV}/bin:"*) ;;' \
      '  *) export PATH="${VIRTUAL_ENV}/bin:/root/.local/bin:${PATH}" ;;' \
      'esac' \
    > /etc/profile.d/capstone-venv.sh

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY dbt_project.yml profiles.yml .bruin.yml Makefile ./
COPY ingest ./ingest
COPY macros ./macros
COPY models ./models
COPY ops ./ops
RUN mkdir -p ./data/seed/events
COPY data/seed/events/events_seed_extended_2015.csv ./data/seed/events/events_seed_extended_2015.csv
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
