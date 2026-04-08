# Stage 1: Trích xuất công cụ từ Alpine
FROM alpine:3.19 AS tools-builder
RUN apk add --no-cache bash curl grep sed

# Stage 2: Trích xuất mc từ MinIO Client chính thức
FROM minio/mc:latest AS mc-builder

# Stage 3: Image MinIO chính thức (Final)
FROM minio/minio:latest

# Copy các công cụ hệ thống cần thiết vào image MinIO siêu tinh gọn
COPY --from=tools-builder /bin/bash /bin/bash
COPY --from=tools-builder /usr/bin/curl /usr/bin/curl
COPY --from=tools-builder /bin/grep /bin/grep
COPY --from=tools-builder /bin/sed /bin/sed
COPY --from=mc-builder /usr/bin/mc /usr/bin/mc

# Tạo symlink để script trong /usr/local/bin có thể gọi được
RUN ln -sf /bin/bash /usr/bin/bash && \
    ln -sf /bin/grep /usr/bin/grep && \
    ln -sf /bin/sed /usr/bin/sed

# Copy configuration và các script khởi tạo của dự án
COPY config/ /root/.minio/
COPY setup-minio.sh /usr/local/bin/setup-minio.sh
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

# Cấp quyền thực thi cho các script
RUN chmod +x /usr/local/bin/setup-minio.sh && \
    chmod +x /usr/local/bin/entrypoint.sh

# Sử dụng entrypoint của dự án để tự động tạo bucket/policy
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
