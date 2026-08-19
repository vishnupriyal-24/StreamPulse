"""
train_anomaly_model.py

Offline training script for StreamPulse anomaly detection.

Reads archived Parquet events from MinIO, trains an Isolation Forest
on the numeric device metrics, and saves the trained model and scaler
to disk for the streaming pipeline.
"""

import boto3
import pandas as pd
import pyarrow.parquet as pq
import io
import joblib

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler


# ============================================================
# CONFIGURATION
# ============================================================

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

BUCKET = "streampulse-raw"
PREFIX = "events/"

FEATURE_COLUMNS = [
    "cpu_usage",
    "memory_usage",
    "temperature",
    "network_latency",
]


# ============================================================
# LOAD DATA FROM MINIO
# ============================================================

def load_parquet_data_from_minio():
    """
    Find all archived Parquet files in MinIO and combine them
    into one pandas DataFrame.
    """

    print("Connecting to MinIO...")

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    response = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix=PREFIX,
    )

    parquet_keys = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    if not parquet_keys:
        raise RuntimeError(
            "No parquet files found in MinIO. "
            "Run the simulator + Spark sinks first."
        )

    print(f"Found {len(parquet_keys)} parquet files.")
    print("Loading archived data...")

    dataframes = []

    for key in parquet_keys:
        print(f"  Reading: {key}")

        obj = s3.get_object(
            Bucket=BUCKET,
            Key=key,
        )

        buffer = io.BytesIO(obj["Body"].read())

        table = pq.read_table(buffer)

        df = table.to_pandas()

        dataframes.append(df)

    combined = pd.concat(
        dataframes,
        ignore_index=True,
    )

    print(f"Loaded {len(combined)} total events.")

    return combined


# ============================================================
# TRAIN ISOLATION FOREST
# ============================================================

def train_model(df):
    """
    Train StandardScaler + Isolation Forest.

    The scaler makes the different metrics comparable.
    Isolation Forest then learns what normal-looking data
    generally looks like and isolates unusual observations.
    """

    print()
    print("Preparing training features...")

    # Make sure required columns exist
    missing_columns = [
        column
        for column in FEATURE_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise RuntimeError(
            f"Missing required columns: {missing_columns}"
        )

    features = df[FEATURE_COLUMNS].dropna()

    if len(features) < 100:
        raise RuntimeError(
            f"Only {len(features)} usable rows found. "
            "Collect more data before training the model."
        )

    print(f"Training rows: {len(features)}")

    # --------------------------------------------------------
    # Scale features
    # --------------------------------------------------------

    scaler = StandardScaler()

    scaled_features = scaler.fit_transform(features)

    # --------------------------------------------------------
    # Train Isolation Forest
    # --------------------------------------------------------

    print("Training Isolation Forest...")

    model = IsolationForest(
        n_estimators=100,
        contamination="auto",
        random_state=42,
    )

    model.fit(scaled_features)

    print("Model training complete.")

    return model, scaler


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 60)
    print("StreamPulse - Anomaly Model Training")
    print("=" * 60)

    # Load archived events
    df = load_parquet_data_from_minio()

    # Train model
    model, scaler = train_model(df)

    # Save model
    joblib.dump(
        model,
        "anomaly_model.joblib",
    )

    # Save scaler
    joblib.dump(
        scaler,
        "anomaly_scaler.joblib",
    )

    print()
    print("=" * 60)
    print("TRAINING SUCCESSFUL")
    print("=" * 60)

    print()
    print("Created:")

    print("  anomaly_model.joblib")
    print("  anomaly_scaler.joblib")

    print()
    print("You can now run:")
    print("  python3 spark_anomaly.py")
    print()


if __name__ == "__main__":
    main()
