"""Clasificador binario trivial para joblib (debe vivir en un módulo, no en __main__)."""

from __future__ import annotations

import numpy as np
from sklearn.base import BaseEstimator, ClassifierMixin


class DegenerateFailureClassifier(BaseEstimator, ClassifierMixin):
    """
    Cuando el train solo tiene una clase (todo éxito o todo fallo): P(fallo) constante.
    Mantiene predict_proba con dos columnas compatible con scoring.
    """

    def fit(self, X, y):
        y = np.asarray(y).astype(int)
        self.classes_ = np.array([0, 1])
        self._p_fail = float(np.clip(np.mean(y), 0.0, 1.0))
        return self

    def predict(self, X):
        return (self.predict_proba(X)[:, 1] >= 0.5).astype(int)

    def predict_proba(self, X):
        n = X.shape[0]
        p1 = np.full(n, self._p_fail, dtype=np.float64)
        return np.column_stack([1.0 - p1, p1])
