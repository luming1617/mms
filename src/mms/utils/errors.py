class MmsError(Exception):
    status_code = 500
    code = "INTERNAL"

    def __init__(self, detail: str, *, code: str | None = None, status_code: int | None = None):
        super().__init__(detail)
        self.detail = detail
        if code:
            self.code = code
        if status_code:
            self.status_code = status_code


class ConsentError(MmsError):
    def __init__(self, code: str, detail: str = "consent is missing, denied or withdrawn"):
        super().__init__(detail, code=code, status_code=403)


class CandidateRejectedError(MmsError):
    """Permanent candidate-level failure: retrying cannot change the outcome.

    Raised for conditions the candidate itself can never satisfy (unknown fact key,
    misconfigured registry). Ingestion must terminate the job with the reason code
    instead of burning retries into the dead letter queue (docs/2 §6.1).
    """

    status_code = 422
    code = "CANDIDATE_REJECTED"

    def __init__(self, reason_code: str, detail: str = ""):
        super().__init__(detail or reason_code, code=reason_code)
        self.reason_code = reason_code


_BARRIER_CODES: dict[str, tuple[str, int]] = {
    "ACTIVE": ("DELETE_IN_PROGRESS", 409),
    "COOLING_DOWN": ("CUSTOMER_DELETED", 410),
    "COMPLETED": ("CUSTOMER_DELETED", 410),
    "STALE_GENERATION": ("STALE_GENERATION", 409),
}


class BarrierError(MmsError):
    """Docs 6.7.1/6.7.6: every barrier verdict carries exactly one reason code."""

    def __init__(self, status: str):
        code, status_code = _BARRIER_CODES[status]
        detail = (
            "carrier is older than the current deletion barrier generation"
            if status == "STALE_GENERATION"
            else f"customer deletion barrier is {status}"
        )
        super().__init__(detail, code=code, status_code=status_code)
        self.barrier_status = status


class BarrierCheckFailedError(MmsError):
    """Fail-closed wrapper for an unreadable barrier (doc 6.7.1 BARRIER_CHECK_FAILED).

    Deliberately not a BarrierError: ingestion must retry on this class instead of
    terminating the job as a permanent barrier rejection.
    """

    def __init__(self, cause: str):
        super().__init__(f"deletion barrier check failed: {cause}", code="BARRIER_CHECK_FAILED", status_code=503)


class NotFoundError(MmsError):
    status_code = 404
    code = "NOT_FOUND"
