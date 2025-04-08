import os
import subprocess
import pandas as pd
import pickle
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV, KFold

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_FILE = os.path.join(BASE_DIR, "data.csv")
MODEL_FILE = os.path.join(BASE_DIR, "model.pkl")

def train_model():
    # Nếu file data.csv không tồn tại, ETL sẽ chạy
    if not os.path.exists(DATA_FILE):
        subprocess.run(["python", os.path.join("app", "etl.py")], check=True)
    
    df = pd.read_csv(DATA_FILE)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values("date")
    
    # --- CHUYỂN ĐỔI CÁC CỘT CHỈ NHẬN GUID (service_id, category_id) THÀNH NUMBER ---
    # Nếu chúng đang ở dạng object (string), hãy chuyển thành categorical và sau đó lấy các codes.
    if df['service_id'].dtype == 'object':
        df['service_id'] = df['service_id'].astype('category')
        df['service_id'] = df['service_id'].cat.codes
    if df['category_id'].dtype == 'object':
        df['category_id'] = df['category_id'].astype('category')
        df['category_id'] = df['category_id'].cat.codes

    # Chọn các feature để train
    feature_cols = ['day_of_week', 'is_weekend', 'promotion_count', 
                    'discount_flag', 'base_price', 'discount_amount',
                    'service_id', 'category_id']
    X = df[feature_cols]
    y = df['booking_count']  # Sử dụng giá trị booking_count gốc

    # Định nghĩa grid cho hyperparameter tuning
    param_grid = {
        'n_estimators': [50, 100, 200],
        'max_depth': [None, 5, 10],
        'min_samples_leaf': [1, 2, 3],
        'max_features': ['sqrt', 'log2']  # Loại bỏ 'auto' vì không được chấp nhận cho regressor
    }
    
    rf = RandomForestRegressor(random_state=42)
    cv = KFold(n_splits=5, shuffle=True, random_state=42)
    
    grid_search = GridSearchCV(rf, param_grid, cv=cv, scoring='neg_mean_absolute_error', n_jobs=-1)
    grid_search.fit(X, y)
    
    best_model = grid_search.best_estimator_
    print("Best Params:", grid_search.best_params_)
    print("Best Score:", grid_search.best_score_)
    
    # Huấn luyện lại trên toàn bộ dữ liệu với best_model
    best_model.fit(X, y)
    
    with open(MODEL_FILE, "wb") as f:
        pickle.dump(best_model, f)
    
if __name__ == "__main__":
    train_model()
