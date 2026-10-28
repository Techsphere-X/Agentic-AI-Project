from fastapi import APIRouter

from app.connectors.diagnostic_llm import get_diagnostic_llm
from app.core.config import get_settings
from app.core.dependencies import ViewerUser
from app.core.exceptions import AppError
from app.diagnosis import hybrid
from app.diagnosis.hybrid import MLUnavailable
from app.diagnosis.log_classifier import classify
from app.schemas.incident import (
    ClassifyLogRequest,
    ClassifyLogResponse,
    DiagnosisRead,
    LLMAnalysis,
    ModelOutput,
)
from app.services import diagnosis_service

router = APIRouter(prefix="/diagnosis", tags=["diagnosis"])

MAX_LOG_BYTES = 256 * 1024


@router.post("/classify", response_model=ClassifyLogResponse)
def classify_log(body: ClassifyLogRequest, _: ViewerUser) -> ClassifyLogResponse:
    """Log Analyzer: regex, model, hybrid decision, and deep AI analysis for a pasted log."""
    if len(body.log.encode("utf-8")) > MAX_LOG_BYTES:
        raise AppError(
            f"Log is larger than {MAX_LOG_BYTES // 1024}KB",
            code="payload_too_large",
            status_code=413,
        )
    settings = get_settings()
    threshold = settings.ML_MIN_CONFIDENCE
    client = diagnosis_service.model_client()
    regex = classify(body.log)
    final, model = hybrid.decide([body.log], client, threshold, regex=regex)
    if model is None and client is not None and final["ml_status"] == "skipped":
        # Regex was specific, so the hybrid did not ask the model; show its opinion anyway.
        try:
            model = client.classify([body.log])
        except MLUnavailable:
            model = None
    regex_only, _ = hybrid.decide([body.log], None, threshold, regex=regex)

    # Deep Generative AI analysis via Qwen 3:4B (Ollama)
    llm_analysis: LLMAnalysis | None = None
    if body.use_llm and settings.DIAGNOSTIC_LLM_ENABLED:
        try:
            analysis_dict = get_diagnostic_llm().analyze(body.log)
            if analysis_dict:
                llm_analysis = LLMAnalysis.model_validate(analysis_dict)
        except Exception:
            llm_analysis = None

    return ClassifyLogResponse(
        regex=DiagnosisRead.model_validate(regex_only),
        model=ModelOutput.model_validate(model, from_attributes=True) if model else None,
        final=DiagnosisRead.model_validate(final),
        ml_status=final["ml_status"],
        threshold=threshold,
        window=getattr(model, "window", None) if model else None,
        llm_analysis=llm_analysis,
    )
