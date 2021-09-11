# this line can be called inside of docker container
torchserve --start --model-store model-store --models sqgen=dino_vitb8.mar --no-config-snapshots
