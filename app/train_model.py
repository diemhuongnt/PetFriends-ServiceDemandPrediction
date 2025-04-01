# app/train_model.py
import os
import subprocess
import pandas as pd
import pickle
from sklearn.ensemble import RandomForestRegressor

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_FILE = os.path.join(BASE_DIR, "data.csv")
MODEL_FILE = os.path.join(BASE_DIR, "model.pkl")

def train_model():
    if not os.path.exists(DATA_FILE):
        subprocess.run(["python", os.path.join("app", "etl.py")], check=True)
    
    df = pd.read_csv(DATA_FILE)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values("date")
    
    feature_cols = ['day_of_week', 'is_weekend', 'promotion_count', 'discount_flag', 'base_price', 'discount_amount', 'service_id', 'category_id']
    X = df[feature_cols]
    y = df['booking_count']
    
    model = RandomForestRegressor(
        n_estimators=100,
        random_state=42,
        min_samples_leaf=2  # Thêm dòng này
    )

    model.fit(X, y)
    
    with open(MODEL_FILE, "wb") as f:
        pickle.dump(model, f)

if __name__ == "__main__":
    train_model()
