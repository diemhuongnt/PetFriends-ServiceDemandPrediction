# Sử dụng Python base image
FROM python:3.9-slim

# Cài đặt các gói hệ thống cần thiết (ví dụ driver ODBC cho SQL Server)
RUN apt-get update && apt-get install -y curl gnupg apt-transport-https unixodbc-dev

# Cài driver ODBC 17 for SQL Server (tùy environment)
# Dưới đây là ví dụ cho Debian/Ubuntu, tham khảo docs Microsoft
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Tạo thư mục /app
WORKDIR /app

# Copy file requirements
COPY requirements.txt .

# Cài đặt các thư viện Python
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ project vào /app
COPY . .

# Tạo user non-root (nếu muốn bảo mật)
# RUN useradd -m appuser
# USER appuser

# Mở port 8000
EXPOSE 8000

# Command chạy FastAPI (main.py) khi container start
CMD ["python", "app/main.py"]
