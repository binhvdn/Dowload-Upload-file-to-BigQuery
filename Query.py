from google.cloud import bigquery
import csv
import os 

# Thiết lập biến môi trường chứa thông tin xác thực
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Python\blockbuster-dccd6-e7cdcaa45ecc.json"

# Khởi tạo client BigQuery
client = bigquery.Client()

# Câu lệnh truy vấn SQL để lấy dữ liệu từ BigQuery
query = """
  select * from `blockbuster-dccd6.test.2006`
"""

# Chạy truy vấn
query_job = client.query(query)

# Đợi cho đến khi truy vấn hoàn thành và lấy kết quả
results = query_job.result()

# Đường dẫn đầy đủ để lưu file CSV vào ổ C
csv_filename = r"C:\Users\Binh\Downloads\2006_test.csv"

# Tạo file CSV mới và ghi dữ liệu vào file đó
with open(csv_filename, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    
    # Ghi tiêu đề cột
    writer.writerow([field.name for field in results.schema])
    
    # Ghi dữ liệu từng dòng
    for row in results:
        writer.writerow(list(row))

print(f"Dữ liệu đã được ghi ra file {csv_filename}")
