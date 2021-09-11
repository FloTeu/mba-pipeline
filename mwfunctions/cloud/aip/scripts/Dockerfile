FROM europe-west1-docker.pkg.dev/mba-pipeline/pytorch-models/ml_model_pytorch_pre_dino

RUN printf 'curl -X POST "http://127.0.0.1:8081/models?url=${MODEL_NAME}.mar&model_name=${MODEL_NAME}&batch_size=128&max_batch_delay=1000&initial_workers=1"' >> /home/model-server/start_model.sh
