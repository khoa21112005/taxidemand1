# Mô tả dự án NYC Taxi Demand

## Mục tiêu

Xây dựng pipeline Big Data có thể kiểm tra và làm sạch dữ liệu chuyến taxi New York, tổng hợp nhu cầu theo khu vực và giờ, huấn luyện mô hình dự báo, rồi trình bày kết quả trong dashboard.

## Dữ liệu và đơn vị phân tích

Dữ liệu nguồn là NYC Yellow Taxi Trip Records theo tháng trong năm 2023 và 2024. Bản demo hiện tại có 78.358.431 chuyến sau làm sạch, 265 taxi zone và 17.544 giờ. Demand grid gồm 4.649.160 cặp zone–giờ; mỗi dòng chứa số chuyến đón được ghi nhận trong giờ đó, bao gồm cả giá trị 0 khi không có chuyến.

Raw Parquet không được đưa vào repository. Dashboard đi kèm các bảng CSV tổng hợp và bảng tra dự đoán để có thể mở giao diện demo ngay sau khi clone.

## Thành phần

- **Hadoop HDFS:** lưu Parquet thô, dữ liệu sạch, demand grid, model và kết quả Spark.
- **Apache Spark / Spark SQL:** làm sạch dữ liệu, thống kê chuyến, tạo demand grid đầy đủ và xuất aggregate.
- **Spark ML:** Random Forest regression dự đoán số chuyến theo zone và giờ.
- **Apache Kafka / Structured Streaming:** đưa sự kiện taxi lên topic, đọc stream và ghi snapshot Parquet vào HDFS.
- **Apache Airflow:** DAG thủ công điều phối các bước batch và kiểm tra đầu ra.
- **Streamlit:** giao diện tổng quan, phân tích, lookup dự đoán, realtime Kafka và kiến trúc hệ thống.

## Luồng xử lý

```text
Yellow Taxi Parquet → HDFS raw → Spark cleaning → Spark SQL aggregates
                                             ↘ zone × hour demand grid
                                                    ↓
                                         Spark ML Random Forest
                                                    ↓
                               model + metrics + prediction lookup
                                                    ↓
                                             Streamlit

Kafka taxi-events → Spark Structured Streaming → HDFS Parquet snapshot
Airflow orchestrates the batch pipeline.
```

## Mô hình dự đoán

Nhãn `demand` là số chuyến đón tại một zone trong một giờ. Bộ đặc trưng hiện tại gồm `zone_id`, giờ, thứ, tháng, `is_weekend` và trung bình nhu cầu lịch sử theo cùng zone–giờ–thứ–tháng. Trung bình lịch sử được tính chỉ từ tập train.

Mô hình là `RandomForestRegressor` của Spark ML, với cấu hình mặc định 20 cây và độ sâu tối đa 8. Dữ liệu được tách theo thời gian: train từ 01/2023 đến 09/2024, validation trong 10/2024 và test trong 11–12/2024. Chỉ số mẫu test hiện có là MAE 6,73, RMSE 22,46 và R² 0,845.

Spark tạo trước prediction lookup cho các kết hợp zone–tháng–thứ–giờ; Streamlit lọc lookup khi người dùng chọn ngày. Vì lookup chỉ chứa tháng, thứ, giờ và zone, năm chọn trên giao diện không làm thay đổi kết quả cho cùng một mẫu lịch.

## Mục đích và giới hạn

Đây là đồ án/demo để thực hành kiến trúc Hadoop, Spark, Kafka, Airflow và ứng dụng phân tích. Producer realtime phát sự kiện mô phỏng; dự đoán là mức cầu kỳ vọng từ mẫu lịch sử và chưa dùng thời tiết, sự kiện, giao thông hoặc số xe đang sẵn có. Kết quả không nên được xem là cam kết nhu cầu vận hành thực tế.

Hướng dẫn cài đặt, chạy pipeline, cấu trúc repository và lệnh sử dụng nằm trong [README chính](../README.md).
