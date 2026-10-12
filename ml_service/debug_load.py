import os
os.environ["HF_HUB_OFFLINE"] = "1"
os.environ["TRANSFORMERS_OFFLINE"] = "1"
import sys
import json
from pathlib import Path

print("Step 1: import torch", flush=True)
import torch
print("Step 2: device check", flush=True)
device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Device is {device}", flush=True)
print("Step 3: import transformers", flush=True)
from transformers import AutoModelForSequenceClassification, AutoTokenizer
print("Step 4: path check", flush=True)
path = Path("weights/20261001-0630")
print("Step 5: loading tokenizer", flush=True)
tokenizer = AutoTokenizer.from_pretrained(path, local_files_only=True)
print("Step 6: tokenizer loaded! loading model...", flush=True)
model = AutoModelForSequenceClassification.from_pretrained(path, local_files_only=True)
print("Step 7: model loaded! moving to device...", flush=True)
if device == "cuda":
    model = model.half()
model = model.to(device).eval()
print("Step 8: model ready on device!", flush=True)
batch = tokenizer(["Traceback (most recent call last):\nConnectionError: 503"], return_tensors="pt").to(device)
with torch.inference_mode():
    logits = model(**batch).logits
print("Step 9: inference done! Logits shape:", logits.shape, flush=True)
