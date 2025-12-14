import os
import numpy as np
import joblib
import tensorflow as tf
import pandas as pd
import matplotlib.pyplot as plt
from data import prepare_df_for_test



def load_models(model_dir="models"):
    autoencoder = tf.keras.models.load_model(f"{model_dir}/autoencoder_final.keras")
    encoder = tf.keras.models.load_model(f"{model_dir}/encoder_final.keras")
    decoder = tf.keras.models.load_model(f"{model_dir}/decoder_final.keras")
    scaler = joblib.load(f"{model_dir}/scaler.pkl")
    return autoencoder, encoder, decoder, scaler


def compute_reconstruction_errors(autoencoder, X):
    reconstructed = autoencoder.predict(X, verbose=0)
    errors = np.mean((X - reconstructed) ** 2, axis=1)
    return errors


def load_threshold(model_dir="models"):
    path = os.path.join(model_dir, "threshold.npy")
    if os.path.exists(path):
        return float(np.load(path))
    return None



def detect_anomalies(errors, threshold=None):
    """
    Si threshold fourni -> on l'utilise (celui du train).
    Sinon fallback -> p95 du fichier courant.
    """
    if threshold is None:
        threshold = np.percentile(errors, 95)

    anomaly_mask = errors > threshold
    anomaly_rate = np.mean(anomaly_mask) * 100
    return anomaly_mask, anomaly_rate, threshold

# ================================================================
# 5) Analyse de la distribution des erreurs
# ================================================================
def analyze_error_distribution(errors, output_dir="results"):
    os.makedirs(output_dir, exist_ok=True)

    # ---- stats ----
    stats = {
        "mean": np.mean(errors),
        "std": np.std(errors),
        "min": np.min(errors),
        "max": np.max(errors),
        "p50": np.percentile(errors, 50),
        "p95": np.percentile(errors, 95),
        "p99": np.percentile(errors, 99),
    }

    print("\n===== ERROR DISTRIBUTION ANALYSIS =====")
    for k, v in stats.items():
        print(f"{k}: {v:.6f}")

    # ---- save CSV ----
    df = pd.DataFrame({"reconstruction_error": errors})
    df.to_csv(f"{output_dir}/reconstruction_errors.csv", index=False)
    print(f"[INFO] CSV saved -> {output_dir}/reconstruction_errors.csv")

    # ---- histogram ----
    plt.figure(figsize=(8, 5))
    plt.hist(errors, bins=50, alpha=0.75)
    plt.title("Histogramme des erreurs de reconstruction")
    plt.xlabel("Erreur (MSE)")
    plt.ylabel("Fréquence")
    plt.grid(True, alpha=0.3)
    plt.savefig(f"{output_dir}/histogram_errors.png")
    plt.close()
    print(f"[INFO] Histogram saved -> {output_dir}/histogram_errors.png")

    # ---- boxplot ----
    plt.figure(figsize=(6, 4))
    plt.boxplot(errors, vert=True)
    plt.title("Boxplot des erreurs de reconstruction")
    plt.ylabel("Erreur (MSE)")
    plt.grid(True, alpha=0.3)
    plt.savefig(f"{output_dir}/boxplot_errors.png")
    plt.close()
    print(f"[INFO] Boxplot saved -> {output_dir}/boxplot_errors.png")

    return stats

def test_anomaly_detection(csv_path="part_19.csv", model_dir="models", schema_path="models/schema.json"):
    # 1) load models
    autoencoder, encoder, decoder, scaler = load_models(model_dir)

    # 2) clean + align EXACTEMENT comme train
    df_test = prepare_df_for_test(csv_path, schema_path=schema_path)

    # 3) scale with train scaler
    X = scaler.transform(df_test.values)
    print(f"[INFO] Test dataset shape: {X.shape}")

    # 4) errors
    errors = compute_reconstruction_errors(autoencoder, X)

    # 5) threshold from train
    threshold = load_threshold(model_dir)

    # 6) detect anomalies
    anomaly_mask, anomaly_rate, threshold = detect_anomalies(errors, threshold)

    print("\n===== ANOMALY DETECTION REPORT =====")
    print(f"File: {csv_path}")
    print(f"Total samples: {len(errors)}")
    print(f"Threshold used: {threshold:.6f}")
    print(f"Detected anomalies: {np.sum(anomaly_mask)}")
    print(f"Anomaly rate: {anomaly_rate:.2f}%")
    stats = analyze_error_distribution(errors)
    return {
        "threshold": threshold,
        "errors": errors,
        "anomaly_mask": anomaly_mask,
        "anomaly_rate": anomaly_rate,
    }


#if __name__ == "__main__":
 #   test_anomaly_detection("tpot-logs/tpot_logs.csv")

