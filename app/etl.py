import os
import sys
import pandas as pd
import pyodbc
import warnings

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_FILE = os.path.join(BASE_DIR, "data.csv")

DB_USERNAME = "petfriends"
DB_PASSWORD = "Admin@123"
DB_SERVER   = "160.250.133.192"
DB_NAME     = "petfriends"
DB_DRIVER   = "ODBC Driver 17 for SQL Server"

conn_str = (
    f"DRIVER={{{DB_DRIVER}}};"
    f"SERVER={DB_SERVER};"
    f"DATABASE={DB_NAME};"
    f"UID={DB_USERNAME};"
    f"PWD={DB_PASSWORD};"
)

def db_has_new_data():
    try:
        conn = pyodbc.connect(conn_str)
        query_max_date = "SELECT MAX(CAST(DateGiven AS DATE)) as max_date FROM [petfriends].[dbo].[AppointmentClinicService]"
        df_max = pd.read_sql(query_max_date, conn)
        conn.close()
        db_max_date = pd.to_datetime(df_max['max_date'][0]).normalize()
        print("DB max date:", db_max_date)
    except Exception as e:
        print("Error querying DB max date:", e)
        return True

    if not os.path.exists(DATA_FILE):
        print("data.csv does not exist. Will update.")
        return True

    try:
        df_existing = pd.read_csv(DATA_FILE, parse_dates=['date'])
        existing_max_date = df_existing['date'].max().normalize()
        print("Existing data.csv max date:", existing_max_date)
        if db_max_date > existing_max_date:
            print("DB has newer data than data.csv. Will update.")
            return True
        else:
            print("data.csv is up-to-date.")
            return False
    except Exception as e:
        print("Error reading data.csv:", e)
        return True

try:
    conn = pyodbc.connect(conn_str)
except Exception as e:
    print("Lỗi kết nối SQL Server:", e)
    raise

query_booking = """
SELECT 
    CAST(apt.StartAt AS DATE) AS date,
    cs.Category AS category_id,
    cs.Id AS service_id,
    cs.Name AS service_name,
    cs.Price AS base_price,
    ISNULL(cs.DiscountAmount, 0) AS discount_amount,
    CAST(cs.DiscountFrom AS DATE) AS discount_from,
    CAST(cs.DiscountTo AS DATE) AS discount_to,
    
    CASE 
        WHEN DATEPART(WEEKDAY, apt.StartAt) = 1 THEN 6
        ELSE DATEPART(WEEKDAY, apt.StartAt) - 2
    END AS day_of_week,
    
    CASE 
        WHEN DATEPART(WEEKDAY, apt.StartAt) IN (1, 7) THEN 1 
        ELSE 0
    END AS is_weekend,
    
    COUNT(DISTINCT p.Id) AS promotion_count,
    
    CASE 
        WHEN cs.DiscountFrom IS NULL OR cs.DiscountTo IS NULL THEN 0
        WHEN apt.StartAt >= cs.DiscountFrom AND apt.StartAt <= cs.DiscountTo THEN 1 
        ELSE 0 
    END AS discount_flag,
    
    COUNT(DISTINCT acs.Id) AS booking_count

FROM 
    [petfriends].[dbo].[AppointmentClinicService] AS acs
JOIN 
    [petfriends].[dbo].[Appointment] AS apt
      ON apt.Id = acs.AppointmentId
JOIN 
    [petfriends].[dbo].[ClinicService] AS cs
      ON acs.ClinicServiceId = cs.Id
LEFT JOIN 
    [petfriends].[dbo].[Promotion] AS p
      ON p.StartDate <= apt.StartAt 
      AND p.EndDate >= apt.StartAt

GROUP BY 
    CAST(apt.StartAt AS DATE),
    cs.Id,
    cs.Category,
    cs.Name,
    cs.Price,
    cs.DiscountAmount,
    cs.DiscountFrom, 
    cs.DiscountTo,
    CASE 
        WHEN DATEPART(WEEKDAY, apt.StartAt) = 1 THEN 6
        ELSE DATEPART(WEEKDAY, apt.StartAt) - 2
    END,
    CASE 
        WHEN DATEPART(WEEKDAY, apt.StartAt) IN (1, 7) THEN 1 
        ELSE 0
    END,
    CASE 
        WHEN cs.DiscountFrom IS NULL OR cs.DiscountTo IS NULL THEN 0
        WHEN apt.StartAt >= cs.DiscountFrom AND apt.StartAt <= cs.DiscountTo THEN 1 
        ELSE 0 
    END

ORDER BY 
    CAST(apt.StartAt AS DATE);
"""

# Đọc dữ liệu booking
df_booking = pd.read_sql(query_booking, conn)
df_booking['date'] = pd.to_datetime(df_booking['date']).dt.normalize()

# Chỉ giữ lại những ngày có booking (không làm cartesian join toàn bộ các ngày trong khoảng)
final_df = df_booking.copy()

# Nếu bạn muốn huấn luyện mô hình chỉ dựa trên dữ liệu có booking thực tế,
# bạn có thể loại bỏ những hàng có booking_count == 0.
final_df = final_df[final_df['booking_count'] > 0].copy()

# Chuyển đổi service_id sang mã số
final_df['service_id'] = final_df['service_id'].astype('category')
final_df['service_id'] = final_df['service_id'].cat.codes

# Tương tự với category_id
final_df['category_id'] = final_df['category_id'].astype('category')
final_df['category_id'] = final_df['category_id'].cat.codes

# Lấy các cột cần thiết
# Giữ đầy đủ các thông tin: date, service_id, service_name, base_price, discount_amount, discount_from, discount_to,
# day_of_week, is_weekend, promotion_count, discount_flag, booking_count
print("Shape of final_df:", final_df.shape)
print(final_df.head())

# Ghi đè file data.csv mới (sẽ có ít dòng hơn, phản ánh chính xác số lượng booking thực tế)
final_df.to_csv(DATA_FILE, index=False)
print("ETL completed, data saved to", DATA_FILE)
