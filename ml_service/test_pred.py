import os
os.environ["HF_HUB_OFFLINE"] = "1"
os.environ["TRANSFORMERS_OFFLINE"] = "1"
import sys
import logging
logging.basicConfig(level=logging.INFO)
from pathlib import Path

print("Finding weights...", flush=True)
from app.model import Predictor, find_weights
w = find_weights(Path("weights"))
print("Found weights:", w, flush=True)
print("Initializing Predictor...", flush=True)
p = Predictor(Path("weights"))
print("Predictor ready:", p.ready, "device:", p.device, "version:", p.model_version, flush=True)
res = p.predict(["Traceback (most recent call last):\nConnectionError: 503 Service Unavailable"])
print("Sample prediction:", res["category"], res["confidence"], flush=True)
