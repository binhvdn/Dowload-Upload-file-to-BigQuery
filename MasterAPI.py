import requests
import csv
from io import StringIO
from datetime import datetime, timedelta
from google.cloud import bigquery
import os

# Tính toán ngày bắt đầu (from date) là ngày hiện tại trừ đi 3 ngày
from_date = (datetime.today() - timedelta(days=3)).strftime('%Y-%m-%d')

# Lấy ngày hiện tại (to date)
to_date = datetime.today().strftime('%Y-%m-%d')

# URL template với placeholder cho from_date và to_date
url_template = "https://hq1.appsflyer.com/api/master-agg-data/v4/app/all?from={}&to={}&groupings=app_id,install_time,pid,c,af_adset,af_ad,geo&kpis=impressions,clicks,installs,cost,revenue,retention_day_1,retention_day_2,retention_day_3,retention_day_4,retention_day_5,retention_day_6,retention_day_7,retention_day_8,retention_day_9,retention_day_10,retention_day_11,retention_day_12,retention_day_13,retention_day_14,retention_day_21,retention_day_30,event_counter_af_inters,event_counter_af_inter,event_counter_af_reward,event_counter_af_rewarded,event_counter_af_purchase,unique_users_af_inters,unique_users_af_inter,unique_users_af_reward,unique_users_af_rewarded,sales_in_usd_af_purchase,activity_revenue"

headers = {
    "accept": "application/json",
    "authorization": "Bearer eyJhbGciOiJBMjU2S1ciLCJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwidHlwIjoiSldUIiwiemlwIjoiREVGIn0.7LhoqKcV8LHjnMB9-kkupAmApYcQnBJD9FhsJvGPMZDnnYV7mm1iCw.ltqxxE7wsNlduA0c.RVLkcwPy8DQi7rckaDD-qMzoPxDBJ6EMLu7h1UNqwut8NDnVgrBlYqm_yBCRQ-MtyAoanGi3rHHjJFRJkXID02yv4aHWo7UQm_n0gTZ1YwYmSAEiXZPUMVGBX-0tahYpzFDFuM0sedKaIFLDXxvHygD_ISYVtAphBPnUZ1NZGKLCE5t26wTOvjZVwbwdhrPHcVFyvh5Yn6YbeM9VHRa1o6dZBW5JF3FyPBECUtUk0FxGqyghpAIjsJGvPWCpvh6ucd1jqnWrRu-vq8ULCtw7nkUcX96JYvbWsLpTR2rEBUmi8iGh3Lp-GUe2uuW83WdybuxtA8ro6C9gOyBXSL6_3_pb1UasFlX4_7evWM6uRE1lJXFndhnEsnYjaJvdM951I_gXCig7Pm0X9c1jKN_nFVgMhihE7nrmnrDqiPeBMiObj_p_1H1zVIRmg3ch14_UeXm3rv-O6fyRfqSmRIRZYbE6ZFKunusOER13lChoOd8FhHadHKbpDPDBZaXlgu3jLbCEdCaHO2lMDE2-ywdwtaU.YJBPzHxlaT5_bty8FG02Dg"  # Thay thế bằng token thực tế
}

# Tạo URL với ngày bắt đầu và ngày hiện tại
url = url_template.format(from_date, to_date)

# Thực hiện request GET
response = requests.get(url, headers=headers)

# Kiểm tra phản hồi từ server
if response.status_code == 200:
    if response.content:
        try:
            # Đọc nội dung CSV từ phản hồi
            csv_content = response.text
            csv_reader = csv.reader(StringIO(csv_content))
            
            # Ghi đè lên tệp CSV cũ
            file_name = 'output16.csv'
            with open(file_name, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                for row in csv_reader:
                    writer.writerow(row)
                    
            print(f"Data from {from_date} to {to_date} has been written to {file_name}")
            
            # Thiết lập client BigQuery
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Python\blockbuster-dccd6-e7cdcaa45ecc.json"  # Đường dẫn đến file JSON của service account
            client = bigquery.Client()

            # Định nghĩa bảng mới
            table_id = "blockbuster-dccd6.test.master_api_test_auto_tatmay"  # Tạo bảng mới với tên "new_table_name"

            # Cấu hình tải dữ liệu lên BigQuery
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,  # Nếu CSV có header, bỏ qua hàng đầu tiên
                autodetect=True,  # Tự động phát hiện schema
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Ghi đè bảng nếu bảng đã tồn tại
            )

            # Tải dữ liệu lên BigQuery và tạo bảng mới
            with open(file_name, "rb") as source_file:
                load_job = client.load_table_from_file(source_file, table_id, job_config=job_config)
                
            load_job.result()  # Chờ cho đến khi job hoàn thành

            print(f"Data from {file_name} has been uploaded to {table_id}")

        except Exception as e:
            print("An error occurred while processing the CSV data:", str(e))
    else:
        print(f"Empty response received for {from_date} to {to_date}")
else:
    print(f"Failed to retrieve data for {from_date} to {to_date}. Status code:", response.status_code)
