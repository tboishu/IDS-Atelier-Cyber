import os
import json
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

DEFAULT_SCHEMA_PATH = "schema.json"


def load_and_clean_csv(path: str, threshold: float = 0.3) -> pd.DataFrame:
    """
    Charge un CSV, supprime les colonnes avec plus de `threshold`
    de valeurs manquantes, impute les NaN numériques par médiane,
    et retourne UNIQUEMENT les colonnes numériques.
    """
    df = pd.read_csv(path)

    # 1) Supprimer colonnes trop vides
    missing_ratio = df.isna().mean()
    cols_to_keep = missing_ratio[missing_ratio < threshold].index
    df = df[cols_to_keep]

    # 2) Imputation NaN sur numériques
    df = df.fillna(df.median(numeric_only=True))

    # 3) Garder seulement le numérique
    df_numeric = df.select_dtypes(include=[np.number]).copy()

    return df_numeric


def save_schema(columns, schema_path: str = DEFAULT_SCHEMA_PATH):
    with open(schema_path, "w") as f:
        json.dump(list(columns), f)


def load_schema(schema_path: str = DEFAULT_SCHEMA_PATH):
    if not os.path.exists(schema_path):
        raise FileNotFoundError(
            f"{schema_path} not found. Train first to create schema."
        )
    with open(schema_path, "r") as f:
        return json.load(f)


def align_columns(df: pd.DataFrame, schema_path: str = DEFAULT_SCHEMA_PATH) -> pd.DataFrame:
    """
    Aligne les colonnes de df sur le schéma d'entraînement :
    - ajoute celles manquantes (remplies par 0)
    - supprime celles en trop
    - réordonne selon le schéma
    """
    schema = load_schema(schema_path)

    # ajouter colonnes manquantes
    for col in schema:
        if col not in df.columns:
            df[col] = 0.0

    # ne garder que celles du schéma + ordre exact
    df = df[schema].copy()
    return df


def fit_scaler(df: pd.DataFrame):
    scaler = MinMaxScaler()
    X = scaler.fit_transform(df.values)
    return X, scaler


def transform_with_scaler(df: pd.DataFrame, scaler: MinMaxScaler):
    return scaler.transform(df.values)


def get_autoencoder_dataset_train(csv_path: str, threshold: float = 0.3):
    """
    Pour l'entraînement :
    - clean CSV
    - retourne df_clean, X_scaled, scaler (fit sur df_clean)
    """
    df_clean = load_and_clean_csv(csv_path, threshold=threshold)
    X, scaler = fit_scaler(df_clean)
    return df_clean, X, scaler


def prepare_df_for_test(csv_path: str, schema_path: str = DEFAULT_SCHEMA_PATH):
    """
    Pour le test :
    - clean CSV identique au train
    - aligne sur schema du train
    - retourne dataframe alignée (NON normalisée)
    """
    df_clean = load_and_clean_csv(csv_path)
    df_aligned = align_columns(df_clean, schema_path=schema_path)
    return df_aligned
