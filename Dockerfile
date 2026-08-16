# syntax=docker/dockerfile:1.7

# ---------------------------------------------------------------------------
# Builder stage
# ---------------------------------------------------------------------------
FROM python:3.14-slim AS builder

WORKDIR /build

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Runtime deps are pinned in pyproject.toml; install the project (with deps)
# into a target dir we copy into the runtime stage. If a hashed
# requirements.lock is added later, prefer:
#   RUN pip install --no-cache-dir --require-hashes --target /wheels -r requirements.lock
COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN pip install --no-cache-dir --target /wheels .

# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
FROM python:3.14-slim AS runtime

# Apply Debian security patches on top of the base. Picks up CVE fixes between
# base rebuilds.
RUN apt-get update && apt-get -y upgrade && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/site-packages \
    PATH=/app/site-packages/bin:$PATH

# Non-root user with pinned UID 1000 (no shell, no home).
RUN groupadd --system --gid 1000 umphreys \
    && useradd --system --uid 1000 --gid 1000 --no-create-home --shell /usr/sbin/nologin umphreys

# Drop pip from the runtime image. Nothing at runtime uses it: dependencies are built
# in the builder stage and reach this stage via PYTHONPATH, and the entrypoint
# and healthcheck are plain `python -m` calls.
#
# This is also the only fix for two recurring Trivy HIGHs. pip ships a vendored
# dependency set (see pip/_vendor/vendor.txt) that Trivy scans as real packages:
# msgpack 1.1.2 (GHSA-6v7p-g79w-8964) and setuptools 70.3.0 (CVE-2025-47273).
# Neither is an application dependency, so no lockfile change can move them, and
# no pip release ships fixed versions. Removing the unused component is the fix.
RUN python -m pip uninstall -y pip \
    && rm -rf /usr/local/lib/python3.*/site-packages/pip \
              /usr/local/lib/python3.*/site-packages/pip-*.dist-info \
              /usr/local/bin/pip /usr/local/bin/pip3 /usr/local/bin/pip3.*

WORKDIR /app
COPY --from=builder /wheels /app/site-packages
COPY migrations/ /app/migrations/
RUN chown -R umphreys:umphreys /app

USER umphreys

EXPOSE 3716

ENTRYPOINT ["python", "-m", "umphreys_vault.cli"]
CMD ["--help"]
