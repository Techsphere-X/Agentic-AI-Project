from fastapi import APIRouter

from app.connectors.diagnostic_llm import get_diagnostic_llm
from app.core.config import get_settings
from app.core.dependencies import ViewerUser
from app.core.exceptions import AppError
from app.diagnosis import hybrid
from app.diagnosis.log_classifier import classify
from app.schemas.incident import (
    ClassifyLogRequest,
    ClassifyLogResponse,
    DiagnosisRead,
    LLMAnalysis,
)

router = APIRouter(prefix="/diagnosis", tags=["diagnosis"])

MAX_LOG_BYTES = 256 * 1024


@router.post("/classify", response_model=ClassifyLogResponse)
def classify_log(body: ClassifyLogRequest, _: ViewerUser) -> ClassifyLogResponse:
    """Log Analyzer: regex rules as a hint, Qwen3 (Ollama) as the only AI model."""
    if len(body.log.encode("utf-8")) > MAX_LOG_BYTES:
        raise AppError(
            f"Log is larger than {MAX_LOG_BYTES // 1024}KB",
            code="payload_too_large",
            status_code=413,
        )
    settings = get_settings()
    regex = classify(body.log)
    regex_only, _ = hybrid.decide([body.log], None, settings.ML_MIN_CONFIDENCE, regex=regex)

    llm_analysis: LLMAnalysis | None = None
    window: str | None = None
    attempted = body.use_llm and settings.DIAGNOSTIC_LLM_ENABLED
    if attempted:
        try:
            analysis_dict = get_diagnostic_llm().analyze(body.log, regex)
            if analysis_dict:
                window = analysis_dict.get("window")
                llm_analysis = LLMAnalysis.model_validate(analysis_dict)
        except Exception:
            llm_analysis = None

    if llm_analysis is not None:
        final = {
            **regex_only,
            "category": llm_analysis.category,
            "label": llm_analysis.label,
            "retryable": llm_analysis.retry_safe,
            "confidence": llm_analysis.confidence,
            "rule": None,
            "matched_line": llm_analysis.evidence_line,
            "source": "model",
            "model_version": llm_analysis.model,
            "ml_status": "used",
        }
    else:
        final = {**regex_only, "ml_status": "unavailable" if attempted else "disabled"}

    return ClassifyLogResponse(
        regex=DiagnosisRead.model_validate(regex_only),
        model=None,
        final=DiagnosisRead.model_validate(final),
        ml_status=final["ml_status"],
        threshold=settings.ML_MIN_CONFIDENCE,
        window=window,
        llm_analysis=llm_analysis,
    )
