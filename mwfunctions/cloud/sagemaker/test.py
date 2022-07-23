from aiohttp_retry import ListRetry
from mwfunctions.asynchronous import get_coroutines_response
from mwfunctions.cloud.sagemaker.classes import MWSagemakerPredictor
from mwfunctions.text.conversion import str_list2b64_str

predictor = MWSagemakerPredictor("roberta-model")
is_ready = get_coroutines_response(predictor.await_until_predictor_is_ready())
corpus = ["Hey my Name is jeff"]
retry_options = ListRetry(timeouts=[2, 2, 2, 2, 2], statuses=[400, 500, 502, 503, 504])
cors = [predictor.apredict({"inputs": str_list2b64_str(corpus)}, retry_options=retry_options, timeout=5) for i in range(30)]
from mwfunctions.profiling import log_time

#retry_options = ExponentialRetry(attempts=2, statuses=[400, 500, 502, 503, 504])
with log_time("Async call Sagemaker"):
    t2 = get_coroutines_response(cors)
t = predictor.predict({"inputs": str_list2b64_str(corpus)})
test = 0