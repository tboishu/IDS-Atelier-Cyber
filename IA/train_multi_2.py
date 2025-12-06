import os
import glob
import numpy as np
import joblib
import pandas as pd
from sklearn.model_selection import train_test_split
import tensorflow as tf
from tensorflow.keras import layers, models, callbacks

from data_2 import (
    load_and_clean_csv,
    save_schema,
    fit_scaler, get_autoencoder_dataset_train

)


# ============================================================
#  LOAD MULTIPLE CSV ROBUSTLY + CLEAN + ALIGN
# ============================================================

def compute_reconstruction_errors(autoencoder, X):
    reconstructed = autoencoder.predict(X, verbose=0)
    errors = np.mean((X - reconstructed) ** 2, axis=1)
    return errors


def load_all_csv_in_folder(folder_path: str, threshold=0.3):
    """
    Charge, nettoie et concatène TOUS les CSV d’un dossier :
    - mêmes colonnes pour tous les fichiers
    - numeric only
    - no NaN / no inf
    - suppression des colonnes constantes
    """

    csv_files = sorted(glob.glob(os.path.join(folder_path, "*.csv")))
    if not csv_files:
        raise FileNotFoundError(f"Aucun CSV trouvé dans : {folder_path}")

    print(f"[INFO] {len(csv_files)} fichiers CSV trouvés")

    dfs = []
    for f in csv_files:
        print(f"[INFO] Chargement : {f}")

        df = load_and_clean_csv(f, threshold=threshold)

        # Remplacement de tout inf
        df = df.replace([np.inf, -np.inf], np.nan)

        dfs.append(df)

    # === Colonnes communes ===
    common_cols = list(set.intersection(*(set(df.columns) for df in dfs)))

    if len(common_cols) == 0:
        raise ValueError("Aucune colonne commune trouvée entre les CSV !")

    print(f"[INFO] Colonnes communes conservées : {common_cols}")

    # Ne garder que celles-ci
    dfs = [df[common_cols].copy() for df in dfs]

    # === Concaténation ===
    df_total = pd.concat(dfs, axis=0, ignore_index=True)
    print(f"[INFO] Total lignes concaténées : {df_total.shape[0]}")

    # === Suppression des colonnes constantes ===
    constant_cols = df_total.columns[df_total.nunique() <= 1]

    if len(constant_cols) > 0:
        print(f"[WARN] Colonnes constantes supprimées : {list(constant_cols)}")
        df_total = df_total.drop(columns=constant_cols)

    # === Nettoyage final ===
    df_total = df_total.replace([np.inf, -np.inf], np.nan)
    df_total = df_total.fillna(df_total.median())

    # Vérification finale
    if df_total.isna().any().any():
        raise ValueError("Des NaN subsistent après nettoyage !")

    # Conversion totale en float (utile pour les modèles ML)
    df_total = df_total.apply(pd.to_numeric, errors="coerce")

    return df_total

# ============================================================
#   AUTOENCODER
# ============================================================

def build_autoencoder(input_dim: int, latent_dim: int = None):
    if latent_dim is None:
        # latent entre 8 et input_dim//8
        latent_dim = max(8, input_dim // 8)

    # ===========================
    #        ENCODER
    # ===========================
    encoder_input = layers.Input(shape=(input_dim,), name="encoder_input")

    x = layers.Dense(input_dim, kernel_regularizer=tf.keras.regularizers.l2(1e-5))(encoder_input)
    x = layers.BatchNormalization()(x)
    x = layers.LeakyReLU(0.1)(x)
    x = layers.Dropout(0.1)(x)

    x = layers.Dense(input_dim // 2, kernel_regularizer=tf.keras.regularizers.l2(1e-5))(x)
    x = layers.BatchNormalization()(x)
    x = layers.LeakyReLU(0.1)(x)

    x = layers.Dense(input_dim // 4, kernel_regularizer=tf.keras.regularizers.l2(1e-5))(x)
    x = layers.BatchNormalization()(x)
    x = layers.LeakyReLU(0.1)(x)

    latent = layers.Dense(latent_dim, activation="linear", name="latent_vector")(x)
    encoder = models.Model(encoder_input, latent, name="encoder")

    # ===========================
    #        DECODER
    # ===========================
    decoder_input = layers.Input(shape=(latent_dim,), name="decoder_input")

    x = layers.Dense(input_dim // 4, kernel_regularizer=tf.keras.regularizers.l2(1e-5))(decoder_input)
    x = layers.BatchNormalization()(x)
    x = layers.LeakyReLU(0.1)(x)

    x = layers.Dense(input_dim // 2, kernel_regularizer=tf.keras.regularizers.l2(1e-5))(x)
    x = layers.BatchNormalization()(x)
    x = layers.LeakyReLU(0.1)(x)

    x = layers.Dense(input_dim, kernel_regularizer=tf.keras.regularizers.l2(1e-5))(x)
    decoder_output = layers.Activation("sigmoid")(x)
    decoder = models.Model(decoder_input, decoder_output, name="decoder")

    # ===========================
    #      AUTOENCODER FULL
    # ===========================
    autoencoder_output = decoder(encoder(encoder_input))
    autoencoder = models.Model(encoder_input, autoencoder_output, name="autoencoder")

    return autoencoder, encoder, decoder



# ============================================================
#   TRAINING PIPELINE
# ============================================================

def train_autoencoder(
    csv_path="dataset/",
    test_size=0.2,
    random_state=42,
    batch_size=256,
    epochs=200,
    model_dir="models",
    schema_path="schema.json"
):
    os.makedirs(model_dir, exist_ok=True)

    # ===============================================
    # 1) CHARGEMENT DES DONNÉES (MULTI-CSV OU SOLO)
    # ===============================================
    if os.path.isdir(csv_path):
        print("[INFO] Mode dossier → Multi-CSV")
        df_clean = load_all_csv_in_folder(csv_path)
        X, scaler = fit_scaler(df_clean)
    else:
        print("[INFO] Mode fichier → CSV unique")
        df_clean, X, scaler = get_autoencoder_dataset_train(csv_path)

    print(f"[INFO] Données finales : {X.shape}")

    # 2) Sauvegarde du schéma et du scaler
    save_schema(df_clean.columns, schema_path=schema_path)
    joblib.dump(scaler, os.path.join(model_dir, "scaler.pkl"))

    # 3) Split train/val
    X_train, X_val = train_test_split(
        X, test_size=test_size, random_state=random_state, shuffle=True
    )
    input_dim = X_train.shape[1]

    # 4) Build autoencoder
    autoencoder, encoder, decoder = build_autoencoder(input_dim)
    autoencoder.compile(
        optimizer=tf.keras.optimizers.Adam(1e-3),
        loss="mse",
        metrics=["mae"]
    )

    autoencoder.summary()

    # 5) Callbacks
    cb_early = callbacks.EarlyStopping(
        monitor="val_loss",
        patience=5,
        restore_best_weights=True
    )
    cb_reduce_lr = callbacks.ReduceLROnPlateau(
        monitor="val_loss",
        factor=0.5,
        patience=3,
        min_lr=1e-6,
        verbose=1
    )
    cb_ckpt = callbacks.ModelCheckpoint(
        filepath=os.path.join(model_dir, "autoencoder_best.keras"),
        monitor="val_loss",
        save_best_only=True,
        verbose=1
    )

    # 6) Entraînement
    history = autoencoder.fit(
        X_train, X_train,
        validation_data=(X_val, X_val),
        epochs=epochs,
        batch_size=batch_size,
        shuffle=True,
        callbacks=[cb_early, cb_reduce_lr, cb_ckpt],
        verbose=1
    )

    # 7) Sauvegarde des modèles finaux
    autoencoder.save(os.path.join(model_dir, "autoencoder_final.keras"))
    encoder.save(os.path.join(model_dir, "encoder_final.keras"))
    decoder.save(os.path.join(model_dir, "decoder_final.keras"))

    # 8) Calcul du seuil basé sur le train
    train_errors = compute_reconstruction_errors(autoencoder, X_train)
    threshold = np.percentile(train_errors, 95)
    np.save(os.path.join(model_dir, "threshold.npy"), threshold)
    print(f"[INFO] Seuil appris sur le train (p95) = {threshold:.6f}")

    print("[INFO] Entraînement terminé avec succès.")
    return autoencoder, encoder, decoder, history


if __name__ == "__main__":
    train_autoencoder("divided/")
