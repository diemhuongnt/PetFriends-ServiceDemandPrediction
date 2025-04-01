import os
import pandas as pd
import pyodbc
import warnings

# Tắt cảnh báo về SQLAlchemy của pandas
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

# Định nghĩa thư mục gốc và file data.csv (sẽ ghi đè file cũ)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_FILE = os.path.join(BASE_DIR, "data.csv")
if os.path.exists(DATA_FILE):
    os.remove(DATA_FILE)

# Thông tin kết nối DB
DB_USERNAME = "petfriends"
DB_PASSWORD = "Admin@123"
DB_SERVER   = "160.30.137.29"
DB_NAME     = "petfriends"
DB_DRIVER   = "ODBC Driver 17 for SQL Server"

conn_str = (
    f"DRIVER={{{DB_DRIVER}}};"
    f"SERVER={DB_SERVER};"
    f"DATABASE={DB_NAME};"
    f"UID={DB_USERNAME};"
    f"PWD={DB_PASSWORD};"
)

try:
    conn = pyodbc.connect(conn_str)
except Exception as e:
    print("Lỗi kết nối SQL Server:", e)
    raise

# Query booking data: lấy booking từ AppointmentClinicService (bao gồm cả service mới)
query_booking = """
SELECT 
    CAST(acs.DateGiven AS DATE) AS date,
    cs.Category AS category_id,
    cs.Id AS service_id,
    cs.Name AS service_name,
    cs.Price AS base_price,
    ISNULL(cs.DiscountAmount, 0) AS discount_amount,
    CAST(cs.DiscountFrom AS DATE) AS discount_from,
    CAST(cs.DiscountTo AS DATE) AS discount_to,
    CASE 
        WHEN DATEPART(WEEKDAY, acs.DateGiven) = 1 THEN 6
        ELSE DATEPART(WEEKDAY, acs.DateGiven) - 2
    END AS day_of_week,
    CASE 
        WHEN DATEPART(WEEKDAY, acs.DateGiven) IN (1, 7) THEN 1 
        ELSE 0
    END AS is_weekend,
    COUNT(DISTINCT p.Id) AS promotion_count,
    CASE 
        WHEN cs.DiscountFrom IS NULL OR cs.DiscountTo IS NULL THEN 0
        WHEN acs.DateGiven >= cs.DiscountFrom AND acs.DateGiven <= cs.DiscountTo THEN 1 
        ELSE 0 
    END AS discount_flag,
    COUNT(DISTINCT acs.Id) AS booking_count
FROM 
    [petfriends].[dbo].[AppointmentClinicService] acs
JOIN 
    [petfriends].[dbo].[ClinicService] cs ON acs.ClinicServiceId = cs.Id
LEFT JOIN 
    [petfriends].[dbo].[Promotion] p ON p.StartDate <= acs.DateGiven AND p.EndDate >= acs.DateGiven
GROUP BY 
    CAST(acs.DateGiven AS DATE),
    cs.Id,
    cs.Category,
    cs.Name,
    cs.Price,
    cs.DiscountAmount,
    cs.DiscountFrom, 
    cs.DiscountTo,
    CASE 
        WHEN DATEPART(WEEKDAY, acs.DateGiven) = 1 THEN 6
        ELSE DATEPART(WEEKDAY, acs.DateGiven) - 2
    END,
    CASE 
        WHEN DATEPART(WEEKDAY, acs.DateGiven) IN (1, 7) THEN 1 
        ELSE 0
    END,
    CASE 
        WHEN cs.DiscountFrom IS NULL OR cs.DiscountTo IS NULL THEN 0
        WHEN acs.DateGiven >= cs.DiscountFrom AND acs.DateGiven <= cs.DiscountTo THEN 1 
        ELSE 0 
    END
ORDER BY 
    CAST(acs.DateGiven AS DATE);
"""
df_booking = pd.read_sql(query_booking, conn)
df_booking['date'] = pd.to_datetime(df_booking['date']).dt.normalize()

# Lấy danh sách service duy nhất từ booking data
services_df = df_booking[['service_id','service_name','category_id','base_price','discount_amount','discount_from','discount_to']].drop_duplicates()

# Sử dụng khoảng thời gian cố định: từ 30 ngày trước đến hôm nay (normalized)
start_date = pd.Timestamp.today().normalize() - pd.Timedelta(days=30)
end_date = pd.Timestamp.today().normalize()
full_dates = pd.date_range(start=start_date, end=end_date, freq='D')
dates_df = pd.DataFrame({'date': full_dates})

# Tạo bảng cartesian giữa full_dates và danh sách service từ booking
dates_df['key'] = 1
services_df['key'] = 1
full_index_df = pd.merge(dates_df, services_df, on='key').drop('key', axis=1)

# Merge booking data vào full_index_df theo (date, service_id, service_name)
merged = pd.merge(full_index_df, df_booking, on=["date", "service_id", "service_name"], how="left", suffixes=('', '_booking'))
merged['booking_count'] = merged['booking_count'].fillna(0)

# Drop các cột dư (các cột có suffix '_booking')
cols_to_drop = [col for col in merged.columns if col.endswith('_booking')]
merged = merged.drop(columns=cols_to_drop)

# Tính các feature từ cột date
merged['day_of_week'] = merged['date'].dt.weekday  # Monday=0,..., Sunday=6
merged['is_weekend'] = merged['date'].dt.weekday.apply(lambda x: 1 if x >= 5 else 0)
merged['promotion_count'] = 0  # Không có dữ liệu promotion trong service reference

# Đảm bảo discount_from và discount_to không NULL: fill bằng default nếu cần
default_date = pd.to_datetime('1900-01-01')
merged['discount_from'] = pd.to_datetime(merged['discount_from']).fillna(default_date)
merged['discount_to'] = pd.to_datetime(merged['discount_to']).fillna(default_date)

# Compute discount_flag: ép kiểu các giá trị về pd.Timestamp trước so sánh
def compute_discount_flag(row):
    discount_from = pd.to_datetime(row['discount_from'])
    discount_to   = pd.to_datetime(row['discount_to'])
    date_value    = pd.to_datetime(row['date'])
    if discount_from == default_date or discount_to == default_date:
        return 0
    return 1 if discount_from <= date_value <= discount_to else 0

merged['discount_flag'] = merged.apply(compute_discount_flag, axis=1)

# Compute final price: nếu discount_flag == 1 thì = base_price - discount_amount, else = base_price
merged['price'] = merged.apply(lambda row: row['base_price'] - row['discount_amount'] if row['discount_flag'] == 1 else row['base_price'], axis=1)

# Lấy các cột cần thiết
final_df = merged.copy()

# Chuyển đổi service_id và category_id sang numeric codes để model có thể học được
final_df['service_id'] = final_df['service_id'].astype('category')
service_mapping = dict(enumerate(final_df['service_id'].cat.categories))
final_df['service_id'] = final_df['service_id'].cat.codes

final_df['category_id'] = final_df['category_id'].astype('category')
category_mapping = dict(enumerate(final_df['category_id'].cat.categories))
final_df['category_id'] = final_df['category_id'].cat.codes

# Debug: In shape và vài dòng để kiểm tra
print("Shape of final_df:", final_df.shape)
print(final_df.head())

# Ghi đè file data.csv mới
final_df.to_csv(DATA_FILE, index=False)
print("ETL hoàn thành, lưu data mới vào", DATA_FILE)
