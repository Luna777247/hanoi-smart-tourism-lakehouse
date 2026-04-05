# Sử dụng phiên bản MinIO cụ thể mới nhất để vá lỗ hổng bảo mật
FROM minio/minio:RELEASE.2024-10-29T16-01-25Z

# Sao chép mc từ image mới nhất
FROM minio/mc:latest AS mc-builder

# Tổng hợp các công cụ từ cả hai base images
FROM minio/minio:RELEASE.2024-10-29T16-01-25Z as final
COPY --from=mc-builder /usr/bin/mc /usr/bin/mc

# Sao chép config và script
COPY config/ /root/.minio/
COPY setup-minio.sh /usr/local/bin/setup-minio.sh
RUN chmod +x /usr/local/bin/setup-minio.sh

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Hạn chế quyền truy cập thư mục chứa script để tăng bảo mật
RUN chmod 755 /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
