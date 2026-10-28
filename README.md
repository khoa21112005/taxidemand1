# NYC Taxi Demand Intelligence

Pipeline Big Data phân tích và dự đoán nhu cầu đón khách taxi theo khu vực và thời gian. Dự án dùng dữ liệu NYC Yellow Taxi 2023–2024, Apache Hadoop HDFS, Apache Spark, Spark ML, Apache Kafka, Apache Airflow và Streamlit.

Dashboard có năm mục: tổng quan, phân tích, dự đoán, dữ liệu Kafka gần thời gian thực và kiến trúc pipeline. Biểu đồ khu vực hiển thị tên zone; trang dự đoán có bảng tra Zone ID, tên khu vực và borough.

## Tổng quan dữ liệu và mô hình

- Dữ liệu đầu vào: 24 tệp Parquet tháng của NYC Yellow Taxi, từ tháng 01/2023 đến tháng 12/2024.
- Sau bước làm sạch, bộ dữ liệu demo ghi nhận **78.358.431 chuyến** hợp lệ.
- Phân vùng không gian: **265 taxi zone**.
- Demand grid: **4.649.160 dòng**, tương ứng 265 zone × 17.544 giờ trong khoảng thời gian dữ liệu.
- Đơn vị dự đoán: số chuyến đón tại một zone trong một giờ.
- Mô hình: `RandomForestRegressor` của Spark ML, cấu hình mặc định 20 cây, độ sâu tối đa 8.
- Tập train: 01/01/2023–30/09/2024; validation: tháng 10/2024; test: 11–12/2024. Việc chia theo thời gian ngăn thông tin tương lai lọt vào train.
- Chỉ số test trong kết quả demo hiện tại: MAE **6,73**, RMSE **22,46**, R² **0,845**.

Các đặc trưng của mô hình gồm `zone_id`, giờ, thứ, tháng, cờ cuối tuần và trung bình nhu cầu lịch sử theo zone–giờ–thứ–tháng. Giá trị lịch sử chỉ được tính từ tập train. Dashboard dùng bảng dự đoán theo lịch được Spark tính và lưu sẵn; lựa chọn năm trên giao diện không làm thay đổi kết quả nếu zone, tháng, thứ và giờ giống nhau.

## Kiến trúc

```text
NYC Yellow Taxi Parquet (2023–2024) ──► HDFS
                                           │
                         Spark kiểm tra và làm sạch dữ liệu
                                           │
                               Spark SQL tổng hợp EDA
                                           │
                         Demand grid: zone × từng giờ
                                           │
                           Spark ML Random Forest
                                           │
                   Model, metrics, dự đoán và lookup Parquet
                                           │
                        Export CSV ──► Streamlit dashboard

Kafka taxi-events ──► Spark Structured Streaming ──► HDFS snapshot
Airflow ──► điều phối pipeline batch từ kiểm tra HDFS đến export dashboard
```

Raw trip data và demand grid đầy đủ được giữ ngoài GitHub. Repository kèm các CSV tổng hợp cùng bảng lookup dự đoán nhỏ để có thể mở dashboard demo mà chưa cần chạy lại pipeline.

## Cấu trúc repository

| Đường dẫn | Nội dung |
| --- | --- |
| `src/01_check_data.py` | Kiểm tra schema và chất lượng các tệp Parquet đầu vào. |
| `src/02_clean_data.py` | Luồng làm sạch dữ liệu cục bộ ban đầu. |
| `src/02_clean_hdfs.py` | Đọc Yellow Taxi từ HDFS, chuẩn hóa và ghi dữ liệu sạch. |
| `src/03_spark_sql.py` | Tổng hợp theo giờ, tháng, zone và loại ngày bằng Spark SQL. |
| `src/04_demand_grid.py` | Tạo đầy đủ các tổ hợp zone × giờ; gán demand bằng 0 khi không có chuyến. |
| `src/05_save_results.py` | Ghi các bảng Parquet tổng hợp lên HDFS. |
| `src/06_train.py` | Tạo đặc trưng, huấn luyện Random Forest, đánh giá và xuất prediction lookup. |
| `src/06_kafka_producer.py` | Phát một mẫu tối đa 1.000 chuyến đã làm sạch lên Kafka. |
| `src/06_realtime_producer.py` | Phát sự kiện taxi mô phỏng để trình diễn luồng realtime. |
| `src/07_kafka_stream.py`, `src/07_realtime_stream.py` | Đọc topic Kafka bằng Spark Structured Streaming, hiển thị console và ghi Parquet lên HDFS. |
| `src/08_export_dashboard.py` | Đưa bảng tổng hợp và kết quả mô hình từ HDFS về `dashboard/data`. |
| `dags/taxi_bigdata_pipeline.py` | Airflow DAG chạy tuần tự các bước batch và kiểm tra đầu ra. |
| `infra/hadoop-cluster/` | Docker Compose và cấu hình HDFS gồm NameNode, ba DataNode. |
| `infra/kafka/` | Docker Compose cho Kafka và giao diện Kafka UI. |
| `dashboard/app.py` | Ứng dụng Streamlit. |
| `dashboard/data/` | CSV tổng hợp, bảng zone, metric và prediction lookup mẫu. |
| `docs/project-description.md` | Tóm tắt mục tiêu, dữ liệu, kiến trúc và giới hạn dự án. |

## Yêu cầu môi trường

- Linux hoặc WSL2 được khuyến nghị; Docker Desktop có tích hợp WSL2.
- Docker Engine và Docker Compose plugin.
- Python 3.10 trở lên và Java 17.
- PySpark/Spark 3.5.1 tương thích với phiên bản Hadoop được chọn.
- Apache Airflow 3 nếu chạy DAG. Airflow được cài riêng vì cách cài phụ thuộc Python và môi trường triển khai.
- Maven hoặc quyền tải package Maven nếu chạy Structured Streaming với Kafka connector.

Cài thư viện Python của dự án:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Đặt `JAVA_HOME`, `HADOOP_HOME`, `HADOOP_CONF_DIR` và `PATH` cho bản Java/Hadoop cài trên máy. Những đường dẫn Hadoop, Java và PySpark trong `dags/taxi_bigdata_pipeline.py` là cấu hình môi trường triển khai; hãy cập nhật chúng khi cài Airflow ở vị trí khác.

## Dữ liệu NYC Taxi

Tải các tệp Yellow Taxi theo tháng từ [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), chọn năm 2023 và 2024. Đặt tệp đúng cấu trúc tên sau:

```text
data/raw/
├── 2023/yellow_tripdata_2023-01.parquet ... yellow_tripdata_2023-12.parquet
└── 2024/yellow_tripdata_2024-01.parquet ... yellow_tripdata_2024-12.parquet
```

Tải bảng phân vùng TLC `taxi_zone_lookup.csv` nếu cần đối chiếu nhãn vị trí. Bản đồ zone dùng trong dashboard mẫu đã nằm tại `dashboard/data/taxi_zone_lookup.csv`.

Để kiểm tra các tệp Parquet tại vị trí tùy chọn:

```bash
spark-submit src/01_check_data.py \
  --data-dir data/raw \
  --report output/data_quality_report.json
```

Script cũng nhận đường dẫn qua biến môi trường `TAXI_DATA_DIR`. Các tệp dữ liệu taxi thô không được lưu trong repository.

## Chạy HDFS và pipeline batch

Khởi chạy HDFS ba DataNode:

```bash
docker compose -f infra/hadoop-cluster/docker-compose.yml up -d
```

Giao diện NameNode nằm tại <http://localhost:9870>. Tạo thư mục HDFS rồi nạp Parquet:

```bash
docker exec taxi-namenode hdfs dfs -mkdir -p /taxi-demand/raw/2023 /taxi-demand/raw/2024
docker cp data/raw/2023/. taxi-namenode:/tmp/taxi-2023/
docker cp data/raw/2024/. taxi-namenode:/tmp/taxi-2024/
docker exec taxi-namenode hdfs dfs -put /tmp/taxi-2023/*.parquet /taxi-demand/raw/2023/
docker exec taxi-namenode hdfs dfs -put /tmp/taxi-2024/*.parquet /taxi-demand/raw/2024/
```

Các script Spark mặc định đọc dữ liệu từ `hdfs://localhost:9000/taxi-demand/raw` và ghi các bảng đã xử lý dưới `hdfs://localhost:9000/taxi-demand`. Hãy cấu hình Hadoop client và Spark trên máy chạy lệnh để kết nối tới HDFS Docker.

Chạy các bước xử lý bằng Spark:

```bash
spark-submit src/02_clean_hdfs.py
spark-submit src/03_spark_sql.py
spark-submit src/04_demand_grid.py
spark-submit src/05_save_results.py
spark-submit src/06_train.py
spark-submit src/08_export_dashboard.py
```

Airflow DAG `taxi_bigdata_pipeline` điều phối theo cùng thứ tự: kiểm tra HDFS, làm sạch, tổng hợp Spark SQL, tạo demand grid, lưu kết quả, huấn luyện, export và kiểm tra đầu ra. DAG hiện cấu hình chạy thủ công (`schedule=None`).

## Mô hình và đầu ra

`src/06_train.py` đọc demand grid Parquet và tạo các cột lịch từ `pickup_hour`. `VectorAssembler` ghép đặc trưng cho Spark ML; `RandomForestRegressor` học nhãn `demand`. Mặc định, model có 20 cây, `maxDepth=8`, lấy mẫu 80% mỗi cây và `seed=42`.

Các đầu ra chính trên HDFS:

| Đường dẫn | Nội dung |
| --- | --- |
| `/taxi-demand/models/random_forest_demand` | Pipeline đã huấn luyện. |
| `/taxi-demand/output/model_metrics` | MAE, RMSE và R² trên validation/test. |
| `/taxi-demand/output/demand_predictions` | Dự đoán và giá trị thực tế của tập test. |
| `/taxi-demand/output/prediction_lookup` | Dự đoán theo tổ hợp zone, tháng, thứ và giờ cho dashboard. |

Dashboard tra bảng lookup đã được Spark tạo sẵn nên thao tác dự đoán không huấn luyện mô hình lại. Hệ thống biểu diễn nhu cầu bằng số chuyến kỳ vọng trong một giờ; giá trị thập phân nhỏ có thể được giao diện làm tròn thành 0 chuyến.

## Chạy Streamlit

Repository có sẵn dữ liệu mẫu ở `dashboard/data`, nên có thể chạy dashboard mà không khởi động HDFS hoặc huấn luyện model:

```bash
source .venv/bin/activate
streamlit run dashboard/app.py --server.address 0.0.0.0 --server.port 8501
```

Mở <http://localhost:8501>. Muốn cập nhật dashboard từ kết quả xử lý mới, chạy `src/08_export_dashboard.py` sau khi đầu ra Spark đã sẵn sàng.

## Chạy Kafka và dữ liệu realtime

Khởi động Kafka và Kafka UI:

```bash
docker compose -f infra/kafka/docker-compose.yml up -d
```

Kafka UI nằm tại <http://localhost:8081>. Topic mặc định là `taxi-events`. Producer lịch sử đọc tối đa 1.000 bản ghi từ HDFS; producer realtime tạo chuyến mô phỏng mỗi giây. Khởi chạy producer phù hợp rồi chạy một trong các Spark streaming consumer:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 src/07_kafka_stream.py
# hoặc phát sự kiện mô phỏng trong một terminal khác
python src/06_realtime_producer.py
```

`src/07_kafka_stream.py` ghi sự kiện tại `/taxi-demand/stream/taxi_events`; `src/07_realtime_stream.py` là luồng demo thứ hai và ghi tại `/taxi-demand/stream/realtime_events`. Cả hai đặt checkpoint riêng trong HDFS. Producer mô phỏng chỉ phục vụ trình diễn, không phải dữ liệu huấn luyện hoặc dữ liệu taxi trực tiếp.

## Giới hạn của bản demo

- Dataset Parquet đầy đủ cần được tải riêng và nạp lên HDFS; GitHub chỉ chứa mã nguồn và artefact demo gọn.
- Prediction lookup dựa vào tháng, thứ và giờ; năm cụ thể không phân biệt hai ngày cùng mẫu lịch.
- Không có đặc trưng thời tiết, sự kiện, cung xe hay tình trạng giao thông.
- Trang Realtime Kafka nhận được sự kiện từ producer sau khi các tiến trình Kafka và Spark Streaming được khởi động; ảnh chụp CSV mẫu không tự cập nhật.
- Kết quả phù hợp cho demo/đồ án và phân tích xu hướng, chưa phải cam kết số chuyến vận hành thực tế.

## Giấy phép và nguồn dữ liệu

Mã nguồn repository phục vụ đồ án Big Data. Dữ liệu chuyến taxi được công bố bởi New York City Taxi and Limousine Commission; vui lòng xem điều khoản, ghi chú và giấy phép được TLC nêu cùng bộ dữ liệu trước khi tái phân phối raw data.
