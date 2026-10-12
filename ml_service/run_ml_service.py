import os
os.environ["HF_HUB_OFFLINE"] = "1"
os.environ["TRANSFORMERS_OFFLINE"] = "1"
os.environ["ML_SERVICE_TOKEN"] = "ml-dev-secret-token"

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ml_service_runner")

from pathlib import Path
from app.config import get_settings
from app.model import Predictor
from app.main import create_app
import uvicorn

if __name__ == "__main__":
    settings = get_settings()
    print("Loading Predictor...", flush=True)
    predictor = Predictor(settings.weights_dir, settings.weights_version, settings.device)
    print(f"Predictor ready={predictor.ready}, device={predictor.device}, version={predictor.model_version}", flush=True)
    app = create_app(predictor=predictor)
    print("Starting uvicorn server on 127.0.0.1:8001...", flush=True)
    uvicorn.run(app, host="127.0.0.1", port=8001, log_level="info")
