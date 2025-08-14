import requests
import csv
from io import StringIO
from datetime import datetime, timedelta
from google.cloud import bigquery
import os

# Thiết lập BigQuery client
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Python\blockbuster-dccd6-e7cdcaa45ecc.json"  # Đường dẫn đến file JSON của service account
client = bigquery.Client()

# Danh sách các app_id
app_ids = ['com.tenmatch.numberpair','com.screw.jam.puzzle','com.tennis.cat.dog','com.nuts.bolts.srcew.puzzle','com.tricky.brainking.puzzle']

# Tính toán ngày bắt đầu (from date) là ngày hiện tại trừ đi 3 ngày
from_date = (datetime.today() - timedelta(days=3)).strftime('%Y-%m-%d')

# Lấy ngày hiện tại (to date)
to_date = datetime.today().strftime('%Y-%m-%d')

# URL template với placeholder cho from_date và to_date
url_template = "https://hq1.appsflyer.com/api/raw-data/export/app/{}/installs_report/v5?from={}&to={}&additional_fields=blocked_reason_rule,store_reinstall,impressions,contributor3_match_type,custom_dimension,conversion_type,gp_click_time,match_type,mediation_network,oaid,deeplink_url,blocked_reason,blocked_sub_reason,gp_broadcast_referrer,gp_install_begin,campaign_type,custom_data,rejected_reason,device_download_time,keyword_match_type,contributor1_match_type,contributor2_match_type,device_model,monetization_network,segment,is_lat,gp_referrer,blocked_reason_value,store_product_page,device_category,app_type,rejected_reason_value,ad_unit,keyword_id,placement,network_account_id,install_app_store,amazon_aid,att,engagement_type,contributor1_engagement_type,contributor2_engagement_type,contributor3_engagement_type,gdpr_applies,ad_user_data_enabled,ad_personalization_enabled"

headers = {
    "accept": "application/json",
    "authorization": "Bearer eyJhbGciOiJBMjU2S1ciLCJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwidHlwIjoiSldUIiwiemlwIjoiREVGIn0.7LhoqKcV8LHjnMB9-kkupAmApYcQnBJD9FhsJvGPMZDnnYV7mm1iCw.ltqxxE7wsNlduA0c.RVLkcwPy8DQi7rckaDD-qMzoPxDBJ6EMLu7h1UNqwut8NDnVgrBlYqm_yBCRQ-MtyAoanGi3rHHjJFRJkXID02yv4aHWo7UQm_n0gTZ1YwYmSAEiXZPUMVGBX-0tahYpzFDFuM0sedKaIFLDXxvHygD_ISYVtAphBPnUZ1NZGKLCE5t26wTOvjZVwbwdhrPHcVFyvh5Yn6YbeM9VHRa1o6dZBW5JF3FyPBECUtUk0FxGqyghpAIjsJGvPWCpvh6ucd1jqnWrRu-vq8ULCtw7nkUcX96JYvbWsLpTR2rEBUmi8iGh3Lp-GUe2uuW83WdybuxtA8ro6C9gOyBXSL6_3_pb1UasFlX4_7evWM6uRE1lJXFndhnEsnYjaJvdM951I_gXCig7Pm0X9c1jKN_nFVgMhihE7nrmnrDqiPeBMiObj_p_1H1zVIRmg3ch14_UeXm3rv-O6fyRfqSmRIRZYbE6ZFKunusOER13lChoOd8FhHadHKbpDPDBZaXlgu3jLbCEdCaHO2lMDE2-ywdwtaU.YJBPzHxlaT5_bty8FG02Dg"  # Thay thế bằng token thực tế
}

# Tạo file CSV
file_name = 'n1_combined.csv'
#write_header = True  # Biến cờ để ghi tiêu đề chỉ một lần

with open(file_name, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)

    # Lặp qua từng app_id và thực hiện request
    for i, app_id in enumerate(app_ids):
        # Tạo URL với app_id và các ngày bắt đầu/kết thúc
        url = url_template.format(app_id, from_date, to_date)

        # Thực hiện request GET
        response = requests.get(url, headers=headers)

        # Kiểm tra phản hồi từ server
        if response.status_code == 200:
            if response.content:
                try:
                    # Đọc nội dung CSV từ phản hồi
                    csv_content = response.text
                    csv_reader = csv.reader(StringIO(csv_content))

                    # Bỏ qua dòng đầu tiên nếu không phải là app_id đầu tiên
                    if i != 0:
                       next(csv_reader, None)  # Bỏ qua dòng tiêu đề

                    # Viết từng hàng vào file CSV (gộp tất cả lại)
                    for row in csv_reader:
                        writer.writerow(row)

                    print(f"Data for app_id {app_id} from {from_date} to {to_date} has been written to {file_name}")

                except Exception as e:
                    print(f"An error occurred while processing the CSV data for app_id {app_id}:", str(e))
            else:
                print(f"Empty response received for app_id {app_id} from {from_date} to {to_date}")
        else:
            print(f"Failed to retrieve data for app_id {app_id} from {from_date} to {to_date}. Status code:", response.status_code)

print(f"All data has been combined into {file_name}")

# Định nghĩa bảng mới trong BigQuery
table_id = "blockbuster-dccd6.test.ggcloudschema"  # Thay thế bằng ID dự án và dataset thực tế của bạn

# Cấu hình tải dữ liệu lên BigQuery
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    #skip_leading_rows=1,  # Bỏ qua hàng đầu tiên nếu CSV có tiêu đề
    autodetect=True,  # Tự động phát hiện schema
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Ghi thêm vào bảng nếu tồn tại
)

# Đọc và upload dữ liệu từ file CSV lên BigQuery
try:
    with open(file_name, "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    
    load_job.result()  # Chờ cho đến khi job hoàn thành

    print(f"Data from {file_name} has been successfully uploaded to {table_id}")

except Exception as e:
    print("An error occurred while uploading the CSV data to BigQuery:", str(e))