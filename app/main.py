# app/main.py
import os
import subprocess
import pickle
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
import logging
from pandas import Timestamp

# Tắt log của APScheduler
logging.getLogger('apscheduler').setLevel(logging.ERROR)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MODEL_FILE = os.path.join(BASE_DIR, "model.pkl")
DATA_FILE = os.path.join(BASE_DIR, "data.csv")

def load_model():
    if not os.path.exists(MODEL_FILE):
        subprocess.run(["python", os.path.join("app", "train_model.py")], check=True)
    with open(MODEL_FILE, "rb") as f:
        model = pickle.load(f)
    return model

app = FastAPI(title="Predict Service Booking API")

class PredictionRequest(BaseModel):
    day_of_week: int
    is_weekend: int
    promotion_count: int
    discount_flag: int
    price: float
    service_id: int
    category_id: int

@app.post("/predict")
def predict_booking(req: PredictionRequest):
    input_data = pd.DataFrame([req.dict()])
    pred = model.predict(input_data)[0]
    return {"predicted_booking_count": int(round(pred))}

@app.get("/servicedemand/predict/next7days")
def predict_next7days():
    df = pd.read_csv(DATA_FILE)
    df['date'] = pd.to_datetime(df['date'])
    
    current_date = Timestamp.today().normalize()
    future_dates = pd.date_range(start=current_date + pd.Timedelta(days=1), periods=7)
    
    services = df.groupby(['service_id','service_name','category_id','base_price','discount_amount','discount_flag'], as_index=False).first()

    predictions = []
    for _, svc in services.iterrows():
        svc_id = svc['service_id']
        cat_id = svc['category_id']
        svc_name = svc['service_name']
        base_price = svc['base_price']
        discount_amount = svc['discount_amount']
        discount_from = pd.to_datetime(svc['discount_from']) if pd.notnull(svc['discount_from']) else None
        discount_to = pd.to_datetime(svc['discount_to']) if pd.notnull(svc['discount_to']) else None
        
        for d in future_dates:
            day_of_week = d.weekday()
            is_weekend = 1 if d.weekday() >= 5 else 0
            promotion_count = 0
            if discount_from is not None and discount_to is not None and discount_from <= d <= discount_to:
                discount_flag = 1
                price = base_price - discount_amount
            else:
                discount_flag = 0
                price = base_price
            
            input_features = {
                "day_of_week": day_of_week,
                "is_weekend": is_weekend,
                "promotion_count": promotion_count,
                "discount_flag": discount_flag,
                "base_price": base_price,  # thêm lại
                "discount_amount": discount_amount,  # thêm lại
                "service_id": svc_id,
                "category_id": cat_id
            }

            predictions.append({
                "date": d.strftime("%Y-%m-%d"),
                **input_features,
                "service_name": svc_name
            })
    
    pred_df = pd.DataFrame(predictions)
    feature_cols = ['day_of_week', 'is_weekend', 'promotion_count',
                'discount_flag', 'base_price', 'discount_amount',
                'service_id', 'category_id']

    pred_df["predicted_booking_count"] = model.predict(pred_df[feature_cols])
    pred_df["predicted_booking_count"] = pred_df["predicted_booking_count"].round().astype(int)
    
    # Tính phần trăm phân bố booking của từng service trong cùng ngày
    total_by_date = pred_df.groupby("date")["predicted_booking_count"].transform("sum")
    pred_df["percentage"] = (pred_df["predicted_booking_count"] / total_by_date * 100).round(2)
    
    pred_df = pred_df.fillna(0)
    return pred_df.to_dict(orient="records")

@app.get("/servicedemand/predict/nextweek")
def predict_next_week():
    df = pd.read_csv(DATA_FILE)
    df['date'] = pd.to_datetime(df['date'])
    today = Timestamp.today().normalize()
    # Find next Monday
    days_ahead = (7 - today.weekday()) % 7
    next_monday = today + pd.Timedelta(days=days_ahead) if days_ahead != 0 else today + pd.Timedelta(days=7)
    future_dates = pd.date_range(start=next_monday, periods=7)
    
    services = df.groupby(['service_id', 'service_name', 'category_id', 'base_price', 'discount_amount', 'discount_from', 'discount_to'], as_index=False).first()
    
    predictions = []
    for _, svc in services.iterrows():
        svc_id = svc['service_id']
        cat_id = svc['category_id']
        svc_name = svc['service_name']
        base_price = svc['base_price']
        discount_amount = svc['discount_amount']
        discount_from = pd.to_datetime(svc['discount_from']) if pd.notnull(svc['discount_from']) else None
        discount_to = pd.to_datetime(svc['discount_to']) if pd.notnull(svc['discount_to']) else None
        
        total_booking = 0
        for d in future_dates:
            day_of_week = d.weekday()
            is_weekend = 1 if d.weekday() >= 5 else 0
            promotion_count = 0
            if discount_from is not None and discount_to is not None and discount_from <= d <= discount_to:
                discount_flag = 1
                current_base_price = base_price - discount_amount
            else:
                discount_flag = 0
                current_base_price = base_price
            
            input_features = {
                "day_of_week": day_of_week,
                "is_weekend": is_weekend,
                "promotion_count": promotion_count,
                "discount_flag": discount_flag,
                "base_price": base_price,
                "discount_amount": discount_amount,
                "service_id": svc_id,
                "category_id": cat_id
            }
            booking_pred = model.predict(pd.DataFrame([input_features]))[0]
            total_booking += booking_pred
        
        predictions.append({
            "service_id": svc_id,
            "service_name": svc_name,
            "category_id": cat_id,
            "total_booking_next_week": int(round(total_booking))
        })
    
    # Define the next week period
    period_str = f"{next_monday.strftime('%Y-%m-%d')} to {(next_monday + pd.Timedelta(days=6)).strftime('%Y-%m-%d')}"
    
    # Attach period info to response
    return {
        "next_week_period": period_str,
        "predictions": predictions
    }

@app.get("/servicedemand/predict/nextmonth")
def predict_next_month():
    df = pd.read_csv(DATA_FILE)
    df['date'] = pd.to_datetime(df['date'])
    today = Timestamp.today().normalize()
    # Get first day of next month
    next_month = (today.replace(day=1) + pd.DateOffset(months=1)).normalize()
    end_of_month = (next_month + pd.DateOffset(months=1)) - pd.Timedelta(days=1)
    num_days = (end_of_month - next_month).days + 1
    future_dates = pd.date_range(start=next_month, periods=num_days)
    
    services = df.groupby(['service_id', 'service_name', 'category_id', 'base_price', 'discount_amount', 'discount_from', 'discount_to'], as_index=False).first()
    
    predictions = []
    for _, svc in services.iterrows():
        svc_id = svc['service_id']
        cat_id = svc['category_id']
        svc_name = svc['service_name']
        base_price = svc['base_price']
        discount_amount = svc['discount_amount']
        discount_from = pd.to_datetime(svc['discount_from']) if pd.notnull(svc['discount_from']) else None
        discount_to = pd.to_datetime(svc['discount_to']) if pd.notnull(svc['discount_to']) else None
        
        total_booking = 0
        for d in future_dates:
            day_of_week = d.weekday()
            is_weekend = 1 if d.weekday() >= 5 else 0
            promotion_count = 0
            if discount_from is not None and discount_to is not None and discount_from <= d <= discount_to:
                discount_flag = 1
                current_base_price = base_price - discount_amount
            else:
                discount_flag = 0
                current_base_price = base_price
            
            input_features = {
                "day_of_week": day_of_week,
                "is_weekend": is_weekend,
                "promotion_count": promotion_count,
                "discount_flag": discount_flag,
                "base_price": base_price,
                "discount_amount": discount_amount,
                "service_id": svc_id,
                "category_id": cat_id
            }
            booking_pred = model.predict(pd.DataFrame([input_features]))[0]
            total_booking += booking_pred
        
        predictions.append({
            "service_id": svc_id,
            "service_name": svc_name,
            "category_id": cat_id,
            "total_booking_next_month": int(round(total_booking))
        })
    
    period_str = f"{next_month.strftime('%B %Y')}"
    
    return {
        "next_month_period": period_str,
        "predictions": predictions
    }

def update_etl_and_retrain():
    try:
        subprocess.run(["python", os.path.join("app", "etl.py")], check=True)
        subprocess.run(["python", os.path.join("app", "train_model.py")], check=True)
        global model
        model = load_model()
    except Exception as e:
        pass

scheduler = BackgroundScheduler()
scheduler.add_job(update_etl_and_retrain, 'interval', hours=0.01, coalesce=True, max_instances=1)
scheduler.start()

@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()

model = load_model()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
