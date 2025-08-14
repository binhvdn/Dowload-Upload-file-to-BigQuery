import requests
import csv
from io import StringIO
from datetime import datetime, timedelta

# Tính toán ngày bắt đầu (from date) là ngày hiện tại trừ đi 3 ngày
from_date = (datetime.today() - timedelta(days=3)).strftime('%Y-%m-%d')

# Lấy ngày hiện tại (to date)
to_date = datetime.today().strftime('%Y-%m-%d')

url_template = "https://hq1.appsflyer.com/api/raw-data/export/app/epic.fight.stick.warriors/installs_report/v5?from={}&to={}&additional_fields=blocked_reason_rule,store_reinstall,impressions,contributor3_match_type,custom_dimension,conversion_type,gp_click_time,match_type,mediation_network,oaid,deeplink_url,blocked_reason,blocked_sub_reason,gp_broadcast_referrer,gp_install_begin,campaign_type,custom_data,rejected_reason,device_download_time,keyword_match_type,contributor1_match_type,contributor2_match_type,device_model,monetization_network,segment,is_lat,gp_referrer,blocked_reason_value,store_product_page,device_category,app_type,rejected_reason_value,ad_unit,keyword_id,placement,network_account_id,install_app_store,amazon_aid,att,engagement_type,contributor1_engagement_type,contributor2_engagement_type,contributor3_engagement_type,gdpr_applies,ad_user_data_enabled,ad_personalization_enabled"

headers = {
    "accept": "text/csv",
    "authorization": "Bearer eyJhbGciOiJBMjU2S1ciLCJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwidHlwIjoiSldUIiwiemlwIjoiREVGIn0.7LhoqKcV8LHjnMB9-kkupAmApYcQnBJD9FhsJvGPMZDnnYV7mm1iCw.ltqxxE7wsNlduA0c.RVLkcwPy8DQi7rckaDD-qMzoPxDBJ6EMLu7h1UNqwut8NDnVgrBlYqm_yBCRQ-MtyAoanGi3rHHjJFRJkXID02yv4aHWo7UQm_n0gTZ1YwYmSAEiXZPUMVGBX-0tahYpzFDFuM0sedKaIFLDXxvHygD_ISYVtAphBPnUZ1NZGKLCE5t26wTOvjZVwbwdhrPHcVFyvh5Yn6YbeM9VHRa1o6dZBW5JF3FyPBECUtUk0FxGqyghpAIjsJGvPWCpvh6ucd1jqnWrRu-vq8ULCtw7nkUcX96JYvbWsLpTR2rEBUmi8iGh3Lp-GUe2uuW83WdybuxtA8ro6C9gOyBXSL6_3_pb1UasFlX4_7evWM6uRE1lJXFndhnEsnYjaJvdM951I_gXCig7Pm0X9c1jKN_nFVgMhihE7nrmnrDqiPeBMiObj_p_1H1zVIRmg3ch14_UeXm3rv-O6fyRfqSmRIRZYbE6ZFKunusOER13lChoOd8FhHadHKbpDPDBZaXlgu3jLbCEdCaHO2lMDE2-ywdwtaU.YJBPzHxlaT5_bty8FG02Dg"
}

#Tạo URL với ngày bắt đầu và ngày hiện tại
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
            file_name = 'organic.csv'
            with open(file_name, mode='w', newline='') as file:
                writer = csv.writer(file)
                for row in csv_reader:
                    writer.writerow(row)

            print(f"Data from {from_date} to {to_date} has been written to {file_name}")
        except Exception as e:
            print("An error occurred while processing the CSV data:", str(e))
    else:
        print(f"Empty response received for {from_date} to {to_date}")
else:
    print(f"Failed to retrieve data for {from_date} to {to_date}. Status code:", response.status_code)