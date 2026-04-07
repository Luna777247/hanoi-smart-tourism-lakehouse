# Sử dụng phiên bản MinIO cụ thể mới nhất để vá lỗ hổng bảo mật
FROM minio/minio:latest

# Sao chép mc từ image mới nhất
FROM minio/mc:latest AS mc-builder

# Tổng hợp các công cụ từ cả hai base images
FROM minio/minio:latest as final
COPY --from=mc-builder /usr/bin/mc /usr/bin/mc

# Cài đặt sed, grep và bash (UBI-minimal use microdnf)
RUN microdnf update -y && \
    microdnf install -y sed grep curl bash && \
    microdnf clean all

# Sao chép config và script
COPY config/ /root/.minio/
COPY setup-minio.sh /usr/local/bin/setup-minio.sh
RUN chmod +x /usr/local/bin/setup-minio.sh

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Hạn chế quyền truy cập thư mục chứa script để tăng bảo mật
RUN chmod 755 /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
