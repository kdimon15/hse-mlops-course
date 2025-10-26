import catboost as cb

class FraudScorer:
    def __init__(self):
        self.model = cb.CatBoostClassifier()
        self.model.load_model("/app/models/catboost_model.cbm")
        self.threshold = 0.42

    def score_batch(self, features):
        probs = self.model.predict_proba(features)[:, 1]
        fraud_flags = (probs > self.threshold).astype(int)
        return probs.tolist(), fraud_flags.tolist()
