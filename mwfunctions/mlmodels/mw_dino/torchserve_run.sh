if [ -z "$MODEL_NAME" ]; then
    echo "Must provide MODEL_NAME in environment" 1>&2
    echo "export MODEL_NAME="
    exit 1
fi

torchserve --start --ncs --model-store ./exporter --ts-config config.properties --foreground --models $MODEL_NAME