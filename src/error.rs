use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KernelErrorKind {
    ClaimConflict,
    SchemaMismatch,
    IdempotencyConflict,
    BudgetExceeded,
    StaleView,
    StaleObservationRun,
    PrivacyPurged,
    OperationExpired,
    NotFound,
    ScopeViolation,
    InvalidSearchQuery,
    MissingInput,
    InvalidInput,
}

#[derive(Debug)]
pub struct KernelError {
    kind: KernelErrorKind,
    message: String,
}

impl KernelError {
    pub fn new(kind: KernelErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn kind(&self) -> KernelErrorKind {
        self.kind
    }

    pub fn claim_conflict(message: impl Into<String>) -> Self {
        Self::new(KernelErrorKind::ClaimConflict, message)
    }

    pub fn schema_mismatch(message: impl Into<String>) -> Self {
        Self::new(KernelErrorKind::SchemaMismatch, message)
    }

    pub fn idempotency_conflict(message: impl Into<String>) -> Self {
        Self::new(KernelErrorKind::IdempotencyConflict, message)
    }

    pub fn budget_exceeded(message: impl Into<String>) -> Self {
        Self::new(KernelErrorKind::BudgetExceeded, message)
    }

    pub fn stale_view(message: impl Into<String>) -> Self {
        Self::new(KernelErrorKind::StaleView, message)
    }

    pub fn stale_observation_run(message: impl Into<String>) -> Self {
        Self::new(KernelErrorKind::StaleObservationRun, message)
    }

    pub fn privacy_purged(message: impl Into<String>) -> Self {
        Self::new(KernelErrorKind::PrivacyPurged, message)
    }

    pub fn operation_expired(message: impl Into<String>) -> Self {
        Self::new(KernelErrorKind::OperationExpired, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(KernelErrorKind::NotFound, message)
    }

    pub fn scope_violation(message: impl Into<String>) -> Self {
        Self::new(KernelErrorKind::ScopeViolation, message)
    }

    pub fn invalid_search_query(message: impl Into<String>) -> Self {
        Self::new(KernelErrorKind::InvalidSearchQuery, message)
    }

    pub fn missing_input(message: impl Into<String>) -> Self {
        Self::new(KernelErrorKind::MissingInput, message)
    }

    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(KernelErrorKind::InvalidInput, message)
    }
}

impl fmt::Display for KernelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for KernelError {}
