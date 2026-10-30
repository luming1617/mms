FROM python:3.13-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
COPY pyproject.toml ./
COPY src ./src
COPY schema ./schema
COPY contracts ./contracts
COPY scripts ./scripts
# Doc 13.6: non-root runtime; the app only writes /app/var (deletion ledger dev default).
RUN pip install --no-cache-dir . \
    && mkdir -p /app/var \
    && chown -R 65532:65532 /app/var
USER 65532
CMD ["mms-api"]
