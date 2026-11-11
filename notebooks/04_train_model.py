# Databricks notebook source
# MAGIC %md
# MAGIC # Gold -> Model
# MAGIC
# MAGIC Trains an XGBoost classifier on the gold feature table and logs it to MLflow.
# MAGIC
# MAGIC The model is deliberately simple -- this project is about the pipeline, not the
# MAGIC model. What matters here is the evaluation:
# MAGIC
# MAGIC - The split is **chronological**, not random. Training on June to predict January
# MAGIC   cannot happen in production and leaks information.
# MAGIC - The headline metric is **PR-AUC**, not accuracy. At a ~0.58% fraud rate, a model
# MAGIC   that predicts "never fraud" scores 99.42% accuracy and catches nothing.
# MAGIC - `scale_pos_weight` reweights the minority class so the loss function stops
# MAGIC   ignoring it.

# COMMAND ----------

# MAGIC %pip install xgboost
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow, time
import pandas as pd
import xgboost as xgb
from sklearn.metrics import (average_precision_score, roc_auc_score,
                             precision_recall_fscore_support, confusion_matrix)

CATALOG       = "workspace"
FEATURE_TABLE = f"{CATALOG}.gold.transaction_features"

FEATURES = ["amt", "amt_vs_card_avg", "card_avg_amt", "amt_sum_24h",
            "txn_count_24h", "txn_count_7d", "card_txn_seq",
            "secs_since_prev_txn", "merch_distance_km",
            "hour_of_day", "day_of_week", "is_night",
            "customer_age", "city_pop"]

# COMMAND ----------

start = time.time()

pdf = (spark.table(FEATURE_TABLE)
       .select(*FEATURES, "is_fraud", "category", "txn_date")
       .toPandas())

pdf = pd.get_dummies(pdf, columns=["category"], prefix="cat")
feature_cols = [c for c in pdf.columns if c not in ("is_fraud", "txn_date")]

# COMMAND ----------

# Chronological split: train on the past, test on the future.
split_date = pdf["txn_date"].quantile(0.8)
train = pdf[pdf["txn_date"] <  split_date]
test  = pdf[pdf["txn_date"] >= split_date]

X_train, y_train = train[feature_cols], train["is_fraud"]
X_test,  y_test  = test[feature_cols],  test["is_fraud"]

scale_pos_weight = (y_train == 0).sum() / (y_train == 1).sum()

# COMMAND ----------

user = spark.sql("SELECT current_user()").collect()[0][0]
mlflow.set_experiment(f"/Users/{user}/fraud_xgboost")

with mlflow.start_run(run_name="baseline_xgb"):
    model = xgb.XGBClassifier(
        n_estimators=200, max_depth=5, learning_rate=0.1,
        scale_pos_weight=scale_pos_weight,
        eval_metric="aucpr", n_jobs=-1, random_state=42)
    model.fit(X_train, y_train)

    probs = model.predict_proba(X_test)[:, 1]
    preds = (probs >= 0.5).astype(int)

    pr_auc  = average_precision_score(y_test, probs)
    roc_auc = roc_auc_score(y_test, probs)
    prec, rec, f1, _ = precision_recall_fscore_support(
        y_test, preds, average="binary", zero_division=0)
    tn, fp, fn, tp = confusion_matrix(y_test, preds).ravel()

    mlflow.log_params({"split_date": str(split_date),
                       "n_features": len(feature_cols),
                       "scale_pos_weight": round(scale_pos_weight, 2)})
    mlflow.log_metrics({"pr_auc": pr_auc, "roc_auc": roc_auc,
                        "precision": prec, "recall": rec, "f1": f1,
                        "true_pos": tp, "false_pos": fp, "false_neg": fn})
    mlflow.xgboost.log_model(model, name="model")

# COMMAND ----------

print(f"split date:   {split_date}")
print(f"train rows:   {len(train):,}  ({y_train.sum():,} fraud)")
print(f"test rows:    {len(test):,}  ({y_test.sum():,} fraud)")
print(f"imbalance:    {scale_pos_weight:.1f}:1")
print(f"PR-AUC:       {pr_auc:.4f}")
print(f"ROC-AUC:      {roc_auc:.4f}")
print(f"precision:    {prec:.4f}")
print(f"recall:       {rec:.4f}")
print(f"F1:           {f1:.4f}")
print(f"caught {tp:,} fraud / missed {fn:,} / false alarms {fp:,}")

top = sorted(zip(feature_cols, model.feature_importances_),
             key=lambda x: -x[1])[:10]
print("\ntop features:")
for name, imp in top:
    print(f"  {name:25s} {imp:.4f}")

print(f"\nelapsed: {time.time() - start:.1f}s")

# A model that cannot beat the base rate is not worth shipping.
assert pr_auc > 0.1, f"PR-AUC {pr_auc:.4f} is implausibly low -- check the feature table."