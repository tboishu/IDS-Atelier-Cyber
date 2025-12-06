import os
import json
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder

DEFAULT_SCHEMA_PATH = "schema.json"


import os
import glob
import numpy as np
import pandas as pd
from sklearn.preprocessing import OneHotEncoder

import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder

def load_and_clean_csv(path: str, threshold: float = 0.3) -> pd.DataFrame:
    """
    Charge un CSV, supprime les colonnes avec plus de `threshold`
    de valeurs manquantes, impute les NaN numériques par médiane,
    encode les colonnes catégorielles en valeurs numériques,
    ajoute un hash de la colonne 'raw',
    et retourne UNIQUEMENT des colonnes numériques.
    """

    df = pd.read_csv(path)

    # 1) Supprimer colonnes trop vides
    missing_ratio = df.isna().mean()
    cols_to_keep = missing_ratio[missing_ratio < threshold].index
    df = df[cols_to_keep]

    # 2) Hash de la colonne raw si elle existe
    if "raw" in df.columns:
        df["raw_hash"] = df["raw"].astype(str).apply(lambda x: hash(x) % (10**9))
        df = df.drop(columns=["raw"])

    # 3) Encodage des colonnes catégorielles
    cat_cols = df.select_dtypes(exclude=[np.number]).columns

    for col in cat_cols:
        le = LabelEncoder()
        df[col] = df[col].astype(str).fillna("Unknown")
        df[col] = le.fit_transform(df[col])

    # 4) Imputation NaN sur colonnes numériques
    df = df.fillna(df.median(numeric_only=True))

    # 5) Garder seulement le numérique
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


def align_columns(df: pd.DataFrame, schema_path: str = DEFAULT_SCHEMA_PATH):
    """
    Aligne les colonnes de df sur le schéma d'entraînement :
    - ajoute celles manquantes (0)
    - supprime celles en trop
    - réordonne dans l'ordre du train
    """
    schema = load_schema(schema_path)

    for col in schema:
        if col not in df.columns:
            df[col] = 0

    df = df[schema].copy()
    return df


def fit_scaler(df: pd.DataFrame):
    scaler = MinMaxScaler()
    X = scaler.fit_transform(df.values)
    return X, scaler


def transform_with_scaler(df: pd.DataFrame, scaler: MinMaxScaler):
    return scaler.transform(df.values)


def get_autoencoder_dataset_train(csv_path: str, threshold: float = 0.3):
    df_clean = load_and_clean_csv(csv_path, threshold=threshold)
    X, scaler = fit_scaler(df_clean)
    return df_clean, X, scaler


def prepare_df_for_test(csv_path: str, schema_path: str = DEFAULT_SCHEMA_PATH):
    df_clean = load_and_clean_csv(csv_path)
    df_aligned = align_columns(df_clean, schema_path=schema_path)
    return df_aligned
