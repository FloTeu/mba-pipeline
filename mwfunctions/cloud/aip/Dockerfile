FROM pytorch/torchserve:0.4.2-gpu

# Setup
# mar file -> /project/model.mar

# model_name
ARG MODEL_NAME
ARG NUM_WORKER

RUN [ ! -z "${MODEL_NAME}" ] # Assert LICENSE_KEY is defined

USER root
# RUN apt install curl
RUN apt-get update; apt-get install curl -y
RUN pip install --upgrade pip && pip install pillow && pip install timm && pip install --upgrade torchvision
COPY . /project
# Eventuell ist die mar file auf nem anderen commit
RUN git clone https://github.com/Flo95x/mba-pipeline.git /project/submodules/mba-pipeline
RUN pip install /project/submodules/mba-pipeline

# Tell torchserve to accept ai-engine request
RUN printf "\nservice_envelope=json\ninstall_py_dep_per_model=true" >> /home/model-server/config.properties
# Copy model to where torchserve expects it
RUN cp /project/${MODEL_NAME}.mar /home/model-server/model-store

RUN printf 'curl -X POST "http://127.0.0.1:8081/models?url=${MODEL_NAME}.mar&model_name=${MODEL_NAME}&batch_size=128&max_batch_delay=1000&initial_workers=1"' >> /home/model-server/start_model.sh

USER model-server

ENV MODEL_NAME="${MODEL_NAME}"

###############''''!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!############
###############
###########
#RUN python -c "import torch;vitb8 = torch.hub.load('facebookresearch/dino:main', 'dino_xcit_small_12_p16')"
RUN python -c "import torch;vitb8 = torch.hub.load('facebookresearch/dino:main', 'dino_vitb8')"

# RUN gcloud source repos clone marketvisionfunctions /home/model-server/submodules/marketvisionfunctions

# Create ENV, cause CMD substitution is @ runtime and does not take ARG from build time

# export MODEL_NAME=dino_xcit_small_12_p16
# torchserve --start --ts-config=/home/model-server/config.properties --models standalone
# sh -c 'curl -X POST 'http://127.0.0.1:8081/models?url=${MODEL_NAME}.mar&model_name=${MODEL_NAME}&batch_size=128&max_batch_delay=1000&initial_workers=1''
# curl "http://localhost:8081/models"
# curl http://localhost:8081/models/${MODEL_NAME}
# curl -v -X PUT "http://localhost:8081/models/${MODEL_NAME}?min_worker=1"
# curl -X DELETE http://localhost:8081/models/${MODEL_NAME}/1.0

CMD ["torchserve", \
     "--start", \
     "--ts-config=/home/model-server/config.properties;", \
     "sleep 5;", \
     "sh /home/model-server/start_model.sh"]
     # "sh -c 'curl -v -X PUT 'http://127.0.0.1:8081/models/${MODEL_NAME}''"]

