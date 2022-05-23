# TRAINING
cd train
python3 train.py
cd ..
# DEPLOYMENT
sh create_model_tar_gz.sh
cd deployment
python3 upload_model_tar_gz.py
python3 deploy.py


