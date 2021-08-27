if [ -z "$MODEL_NAME" ]; then
    echo "Must provide MODEL_NAME in environment" 1>&2
    echo "export MODEL_NAME="
    exit 1
fi


torch-model-archiver --model-name $MODEL_NAME \
    --serialized-file /home/r_beckmann/experiments/DinoLinearCosine/ead0b372be8f4375a4cc1063b5f51387/checkpoints/checkpoint.pth \
    --handler ./custom_trained_xcit_handler.py \
    --model-file model.py \
    --export-path exporter/ -v "1.0" -r ./serve_requirements.txt -f --extra-files vision_transformer.py,utils.py,preprocessing.py