# Use the official MinIO image as the base image
FROM quay.io/minio/minio:latest

# Set the working directory inside the container
WORKDIR /root

# Expose the necessary ports
EXPOSE 9000 9001

# Set the default command to start MinIO server and initialize the bucket
CMD ["sh", "-c", "minio server /data --console-address ':9001' & sleep 5 && \
                   mc alias set myminio http://localhost:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD} && \
                   mc mb -p myminio/${MLFLOW_BUCKET_NAME} && \
                   tail -f /dev/null"]
