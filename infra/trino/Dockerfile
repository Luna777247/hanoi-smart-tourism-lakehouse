FROM trinodb/trino:436

# Copy files
COPY docker-entrypoint.sh /root/docker-entrypoint.sh
COPY install.properties /root/install.properties
COPY ./config/catalog /etc/trino/catalog

# Install dos2unix để fix line endings (nếu build trên Windows)
USER root
RUN microdnf install -y dos2unix || true

# Fix line endings và set quyền thực thi
RUN dos2unix /root/docker-entrypoint.sh 2>/dev/null || sed -i 's/\r$//' /root/docker-entrypoint.sh
RUN chmod +x /root/docker-entrypoint.sh

# Set working directory
WORKDIR /root

ENTRYPOINT ["/root/docker-entrypoint.sh"]