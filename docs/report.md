# BÁO CÁO DỰ ÁN: XÂY DỰNG NỀN TẢNG KHO DỮ LIỆU THÔNG MINH CHO DU LỊCH HÀ NỘI

(HANOI SMART TOURISM DATA LAKEHOUSE PLATFORM)

---

## CHƯƠNG 1: GIỚI THIỆU DỰ ÁN

### 1.1. Tổng quan dự án

**Hanoi Smart Tourism Data Lakehouse Platform** là nền tảng dữ liệu hiện đại được thiết kế nhằm hỗ trợ quản lý và phát triển du lịch bền vững tại Thủ đô Hà Nội. Dự án tập trung vào việc thu thập, xử lý, lưu trữ và phân tích dữ liệu từ nhiều nguồn về các điểm du lịch (attractions), giúp các cơ quan quản lý theo dõi hiệu suất thực tế của từng điểm đến thông qua các chỉ số quan trọng như rating, số lượng đánh giá, xu hướng biến động, phân bố theo quận/huyện và nguy cơ quá tải du khách.

### 1.2. Bối cảnh và tính cấp thiết

Trong những năm gần đây, Hà Nội đã có nhiều nỗ lực chuyển đổi số ngành du lịch. Tuy nhiên, vẫn tồn tại một số hạn chế:

- **Dữ liệu tĩnh**: Các cổng thông tin hiện nay chủ yếu phụ thuộc vào cập nhật thủ công, thiếu khả năng nắm bắt phản hồi thực tế từ du khách trên các nền tảng mạng xã hội và bản đồ số.
- **Độ trễ thông tin**: Các báo cáo thống kê định kỳ thường có độ trễ cao (theo tháng/quý), khó xử lý kịp thời các tình huống như quá tải tại điểm đến.
- **Thiếu hạ tầng Big Data**: Các nghiên cứu hiện tại thường dừng ở mức prototype, thiếu hạ tầng mạnh mẽ để xử lý hàng triệu bản ghi đa nguồn.

Dự án này đóng vai trò là **trục hạ tầng dữ liệu thông minh**, bổ sung cho hệ sinh thái du lịch quốc gia Visit Vietnam bằng khả năng giám sát thời gian thực.

### 1.3. Mục tiêu dự án

- **Mục tiêu tổng quát**: Xây dựng nền tảng Data Lakehouse làm trục hạ tầng dữ liệu thông minh cho du lịch Thủ đô.
- **Mục tiêu cụ thể**:
  - Tự động hóa quy trình thu thập dữ liệu đa nguồn theo lịch trình (Scheduled Batch Pull API) thông qua Airflow DAGs.
  - Triển khai kiến trúc Medallion (Bronze → Silver → Gold) trên Apache Iceberg.
  - Đảm bảo chất lượng và quản trị dữ liệu (Governance & Quality).
  - Phát triển giao diện Management Portal và Analytics Dashboard chuyên sâu.

### 1.4. Phạm vi và Ý nghĩa

- **Phạm vi**: Tập trung vào lớp dữ liệu điểm du lịch (attractions) tại Hà Nội.
- **Ý nghĩa khoa học**: Mô hình tham chiếu thực tiễn cho kiến trúc Lakehouse và Big Data Analytics trong du lịch bền vững.
- **Ý nghĩa thực tiễn**: Giúp cơ quan quản lý chuyển từ "đợi báo cáo" sang "chủ động dự báo" và ra quyết định dựa trên dữ liệu thực.

### 1.5. Cấu trúc báo cáo

Báo cáo gồm 4 chương chính cùng phần Kết luận, Tài liệu tham khảo và Phụ lục, trình bày logic từ cơ sở lý thuyết đến triển khai thực nghiệm.

---

## CHƯƠNG 2: CƠ SỞ LÝ THUYẾT VÀ TỔNG QUAN CÔNG NGHỆ

### 2.1. Kiến trúc Data Lakehouse & Medallion

- **Data Lakehouse**: Là một kiến trúc quản lý dữ liệu mới, kết hợp tính linh hoạt và chi phí thấp của Data Lake với khả năng quản trị, hiệu năng và tính toàn vẹn của Data Warehouse [1]. Theo Armbrust và cộng sự (2021), Lakehouse cho phép hỗ trợ đồng thời cả Machine Learning và Business Intelligence trên một nền tảng duy nhất, loại bỏ tình trạng silo dữ liệu.
- **Medallion Architecture**: Đây là mô hình thiết kế dữ liệu logic do Databricks đề xuất nhằm cải thiện chất lượng dữ liệu dần dần qua các tầng [2]:
  - **Bronze (Raw)**: Lưu trữ dữ liệu gốc "as-is" từ nguồn, đóng vai trò là nhật ký kiểm toán (audit trail) bất biến.
  - **Silver (Cleansed)**: Dữ liệu đã được làm sạch, chuẩn hóa và kiểm tra chất lượng dựa trên các quy tắc nghiệp vụ.
  - **Gold (Curated)**: Dữ liệu đã được tổng hợp theo mô hình Star Schema, tối ưu cho việc truy vấn báo cáo và phân tích chuyên sâu.

### 2.2. Công nghệ lưu trữ Apache Iceberg

Được phát triển ban đầu tại **Netflix (2017)** bởi Ryan Blue và Dan Weeks để giải quyết các hạn chế về hiệu năng và tính tin cậy của định dạng Hive truyền thống [3]. **Apache Iceberg** cung cấp các tính năng cấp cơ sở dữ liệu cho Data Lake:

- **ACID Transactions**: Đảm bảo tính nhất quán khi nhiều engine (Spark, Trino) cùng đọc và ghi đồng thời.
- **Hidden Partitioning**: Tự động quản lý phân vùng, giúp người dùng không cần lo lắng về việc tối ưu hóa thủ công các truy vấn.
- **Time Travel**: Lưu trữ lịch sử snapshot, cho phép "du hành thời gian" để kiểm tra dữ liệu tại một thời điểm bất kỳ trong quá khứ.

### 2.3. Các nguồn dữ liệu Ingestion

- **Google Local/Places API**: Nguồn cung cấp dữ liệu về xếp hạng (rating) và đánh giá (reviews) thực tế lớn nhất thế giới cho các điểm đến. Để tối ưu hạn mức (Quota Limit) do giới hạn API Key nội bộ, dự án kết hợp khai thác bằng việc gọi trực tiếp đến Google REST API (`v1/places:searchNearby`) và gọi thông qua các cổng trung gian (RapidAPI) tích hợp cơ chế xoay vòng khóa (Key Rotation) trong module Enrichment.
- **OpenStreetMap (OSM) & Overpass API**: Nguồn dữ liệu bản đồ mở, cung cấp tọa độ chính xác và các thẻ (tags) thông tin địa lý đa dạng.

*Lưu ý: Quy trình thu thập dữ liệu tuân thủ nghiêm ngặt các điều khoản sử dụng (ToS) của nhà cung cấp và chỉ khai thác các trường thông tin được phép công khai.*

### 2.4. Quản trị dữ liệu (Data Governance)

Tích hợp **OpenMetadata** dựa trên chuẩn **OpenLineage** để xây dựng hệ sinh thái dữ liệu có khả năng quan sát (observability) [4]:

- **Data Catalog**: Cung cấp một giao diện tập trung để khám phá và hiểu ý nghĩa của dữ liệu.
- **Data Lineage**: Tự động ghi nhận luồng di chuyển của dữ liệu thông qua các sự kiện (Events) được gửi từ Spark và Airflow, giúp thực hiện phân tích tác động (impact analysis) khi có thay đổi.

---

## CHƯƠNG 3: THIẾT KẾ KIẾN TRÚC HỆ THỐNG

### 3.1. Kiến trúc tổng thể (Containerized Ecosystem)

Hệ thống được thiết kế theo kiến trúc Microservices, đóng gói hoàn chỉnh trong các Container Docker nhằm đảm bảo tính nhất quán giữa môi trường phát triển và vận hành. Các thành phần chính bao gồm:

- **Lớp Lưu trữ (Storage Layer)**: Sử dụng **MinIO** giả lập chuẩn S3 API, lưu trữ các file Parquet của Iceberg.
- **Lớp Tính toán (Computing Layer)**: Phối hợp giữa **Apache Spark** (xử lý Batch/Stream nặng) và **Trino** (xử lý truy vấn SQL tương tác).
- **Lớp Điều phối (Orchestration)**: **Apache Airflow 3.1.0** quản lý các DAG (Directed Acyclic Graphs), kết hợp với **dbt Cosmos** để điều phối các mô hình dữ liệu một cách linh hoạt [5].

### 3.2. Thiết kế các lớp dữ liệu Medallion

Mô hình Medallion trong dự án được tinh chỉnh để tối ưu cho dữ liệu du lịch không cấu trúc:

1. **Bronze (Raw Zone)**:
    - **Dữ liệu mộc**: Lưu tệp JSON nguyên bản (immutable) lấy từ Google/OSM API lên MinIO.
    - **Bảng Bronze**: Sử dụng PySpark đọc tệp JSON, chuyển sang định dạng bảng Iceberg sơ khai, bổ sung thêm metadata (`ingested_at`).
2. **Silver (Enriched Zone)**:
    - Thực hiện **Data Cleaning**: Loại bỏ các địa điểm ngoài ranh giới địa lý Hà Nội dựa trên tọa độ (GPS bounding box) và chuẩn hóa định dạng tên (Title Case).
    - **Enrichment**: Bố sung điểm chất lượng dữ liệu (`data_quality_score`) phân loại tính tin cậy dựa trên số lượng nguồn dữ liệu chéo cung cấp.
3. **Gold (Analytics Zone)**:
    - Xây dựng các mô hình dữ liệu Gold bằng Spark jobs mục tiêu (`mart_district_stats`, `mart_tourism_heatmap`).
    - Phân bổ thông tin hành chính (quận/huyện) bằng quy tắc tọa độ không gian trực tiếp trong SQL.
    - Cung cấp dữ liệu đã gia công sẵn sàng cho các công cụ BI tiêu thụ (Trino / Superset / Folium).

### 3.3. Chiến lược hợp nhất dữ liệu (Deduplication & DataMerger)

Thách thức lớn nhất là một địa điểm (ví dụ: Văn Miếu) có thể xuất hiện ở các nguồn với tên gọi khác nhau. Hệ thống sử dụng module `DataMerger` chuyên biệt:

- **Cross-check Không gian và Ngữ nghĩa**: Hệ thống chạy hai lớp bảo vệ chống trùng lặp. (1) Nếu khoảng cách GPS dưới 100m, tự động gộp (đại diện cho cùng một khu vực di tích). (2) Nếu khoảng cách trong khoảng 100m - 300m, hệ thống chỉ gộp khi và chỉ khi tên địa điểm có sự tương đồng Levenshtein trên 80%. Điều này giúp bắt được các điểm sai lệch tọa độ diện rộng nhưng vẫn chung một tên gọi thống nhất.
- **Ưu tiên nguồn (Source Priority)**: Ưu tiên tọa độ từ OSM (độ chính xác địa lý cao) và thông tin đánh giá (rating) từ Google Places (độ phủ người dùng lớn).
- **Thuật toán quét cuốn chiếu (Rolling Batch Offset)**: Do giới hạn Rate Limit của Google (RapidAPI), module `OSMGoogleEnrichor` áp dụng logic tính toán bù trừ theo ngày trong năm (`(day_of_year * batch_size) % total_candidates`) để mỗi ngày chỉ Enrich một nhóm nhỏ (batch) địa điểm. Kết hợp xoay vòng API Keys, qua đó "quét" toàn bộ bản đồ theo vòng lặp 30 ngày mà không mất thêm chi phí mua API.

### 3.4. Giải pháp Unified Catalog và Secrets Management

- **Unified Catalog**: Thông qua **JDBC Iceberg Catalog**, hệ thống sử dụng PostgreSQL làm "Source of Truth" cho toàn bộ thông tin về bảng. Điều này cho phép Spark viết dữ liệu và Trino có thể đọc được ngay lập tức mà không có độ trễ metadata.
- **Bảo mật**: Sử dụng **HashiCorp Vault**. Các API Key nhạy cảm không bao giờ xuất hiện ở dạng văn bản thuần túy trong mã nguồn mà được truy xuất động qua Vault Token trong lúc runtime.

### 3.5. Chiến lược Fallback và Đảm bảo tính liên tục (Data Resilience)

Trong các hệ thống thu thập dữ liệu từ bên thứ ba (Google, OSM), rủi ro lớn nhất là sự gián đoạn do hết hạn mức API (Quota Exceeded) hoặc lỗi mạng. Dự án triển khai cơ chế **Fallback** thông qua module `FallbackManager`:

- **Dữ liệu hạt giống (Seed Data)**: Lưu trữ tại thư mục `data/seed` dưới dạng các tệp JSON được trích xuất và kiểm định vào tháng 03/2026. Đây là "lưới an toàn" đảm bảo hệ thống luôn có dữ liệu sạch để vận hành khi tất cả API đều ngưng hoạt động.
- **Failover & Load Balancing**: Trong `OSMCollector`, hệ thống được thiết lập mảng danh sách các máy chủ gương (Overpass Mirrors). Khi một máy chủ báo quá tải (HTTP 429/504), hệ thống lập tức nhảy sang truy vấn mirror tiếp theo.
- **Redis Distributed Caching**: Dữ liệu từ OSM vốn là những dữ liệu ít thay đổi tĩnh, việc gọi liên tục là thừa thãi. Hệ thống sử dụng Redis Cache để lưu kết quả trả về của một truy vấn với TTL = 7 ngày (604.800 giây), giảm thiểu đến 90% số lượng request lãng phí ra mạng lưới quốc tế.

### 3.6. Hệ thống giao diện điều hành và Phân tích (Serving Layer)

Cung cấp hai mức độ tương tác cho người dùng:

1. **Management Portal (Next.js & FastAPI)**: Cung cấp giao diện vận hành để theo dõi sức khỏe hệ thống, quản lý cấu hình và kích hoạt các job thủ công.
    - Quản lý và kích hoạt (Trigger) các Pipeline Ingestion và dbt chuyển đổi.
    - Trực quan hóa các thống kê thời gian thực (Real-time Stats) dưới dạng KPI Counters như: Tổng số điểm đến, Tổng số Quận/Huyện có điểm du lịch, Điểm đánh giá trung bình, và Chỉ số nguy cơ quá tải (Overcrowded Count).
2. **Analytics Dashboard (Apache Superset)**: Định hướng khai thác báo cáo chuyên sâu thông qua giao diện BI kéo thả từ các bảng Gold.
3. **Stand-alone Spatial Visualization**: Sử dụng Python script (`visualize_heatmap.py`) kết nối với Trino thông qua thư viện `folium` để xuất trực tiếp các Bản đồ nhiệt (Heatmap) định dạng HTML vô hướng.

---

## CHƯƠNG 4: TRIỂN KHAI VÀ KẾT QUẢ THỰC NGHIỆM

### 4.1. Môi trường triển khai và Tối ưu hóa hạ tầng

Hệ thống được vận hành trên nền tảng Modern Data Stack với các cấu hình tối ưu sau:

- **Nền tảng**: Docker Engine 24.0+ trên Ubuntu 22.04 LTS.
- **Tối ưu hóa Airflow**: Triển khai Airflow 3.1.0 cùng mô hình TaskFlow API và Cosmos [5]. Việc sử dụng `uv package manager` giúp tối ưu hóa thời gian build và khởi động worker đáng kể.
- **Tối ưu hóa Iceberg (Data Retention Policy)**: Không chỉ nén (Compaction) xuống 128MB để truy vấn qua Trino nhanh hơn 40%, DAG `util_maint_iceberg` còn tự động thực thi dọn dẹp các lịch sử Snapshot cũ (`expire_snapshots`) và các tệp Parquet mồ côi (`remove_orphan_files`) quá hạn 7 ngày, giúp tiết kiệm dung lượng MinIO lên đến 60% theo thời gian.
- **Frontend Real-time Sync**: Ở Lớp Serving, Management Portal tận dụng `TanStack React Query` với tùy chọn `refetchInterval: 30_000`, liên tục giữ luồng kết nối polling 30 giây một lần với Backend FastAPI, bảo đảm Dashboard chỉ số luôn hiện sống động mà không cần nạp lại trang.
- **Quản lý Secrets**: 100% các API Key và thông tin kết nối Database được bảo mật qua **HashiCorp Vault**, loại bỏ hoàn toàn việc lưu trữ plaintext trong biến môi trường.

### 4.2. Kết quả thu thập và Hợp nhất dữ liệu thực tế

Dự án đã thực hiện thu thập dữ liệu tại khu vực Hà Nội với các kết quả định lượng:

- **Số lượng địa điểm**: Thu thập thành công **115 địa điểm** du lịch trọng điểm (Danh lam thắng cảnh, Di tích lịch sử, Bảo tàng).
- **Hợp nhất dữ liệu**: Module `DataMerger` đã xử lý gộp thành công các bản ghi trùng lặp từ Google và OSM. Ví dụ: "Văn Miếu - Quốc Tử Giám" (Google) và "Temple of Literature" (OSM) được nhận diện là một thực thể duy nhất nhờ tọa độ và thuật toán Fuzzy.
- **Tổng hợp theo khu vực**: Thông qua mart `mart_district_stats`, hệ thống tự động tổng hợp số lượng điểm đến, điểm đánh giá trung bình và tổng lượt review theo quận/huyện.

### 4.3. Đánh giá hiệu năng và Quản trị

1. **Tốc độ xử lý**: Pipeline từ Bronze đến Gold cho 1.000 bản ghi mất trung bình **4-6 phút** tùy thuộc vào độ trễ của API nguồn.
2. **Độ tin cậy dữ liệu**: Hệ thống thực hiện kiểm định chất lượng tự động trực tiếp trên **PySpark DataFrame API** thông qua các bước filter ở tầng Silver:
    - Lọc bỏ triệt để các dữ liệu khuyết thiếu tọa độ (`col("lat").isNotNull()`).
    - Đảm bảo điểm đến thực sự nằm trong ranh giới địa lý Hà Nội (`col("lat").between(20.5, 21.5) & col("lon").between(105.0, 106.5)`).
3. **Giao diện OpenMetadata**: Hiển thị đầy đủ **Mạng nhện (Lineage)**. Người quản trị có thể truy vết ngược (trace back) một bản ghi tại bảng Gold từ tầng Silver enriched và ngược về tận gốc các báo cáo (Google/OSM) như thế nào.

### 4.4. Kết quả trực quan hóa (Visualization)

Thông qua Management Portal (Next.js), Python Scripts và Apache Superset, hệ thống đã cung cấp:

- **Live Lakehouse Stats**: Giao diện điều hành tức thời báo cáo tổng số điểm đến, đánh giá trung bình, nguy cơ quá tải cập nhật mỗi 30 giây từ Frontend App tới Backend FastAPI.
- **Bản đồ nhiệt (Heatmap) bằng Folium**: Hiển thị mật độ điểm du lịch tập trung tại các quận trung tâm. Khởi tạo thủ công thông qua thư viện Python Folium (`scripts/visualize_heatmap.py`) truy vấn trực tiếp engine Trino.
- **Biểu đồ Analytics**: Khai thác trên Apache Superset để so sánh và theo dõi chất lượng dịch vụ giữa các nhóm bảng Dimension/Fact.

---

## KẾT LUẬN

### 1. Tóm tắt kết quả

Dự án đã xây dựng thành công nền tảng **Hanoi Smart Tourism Data Lakehouse** vận hành ổn định. Hệ thống không chỉ giải quyết bài toán thu thập dữ liệu đa nguồn mà còn thiết lập một quy chuẩn quản trị dữ liệu hiện đại, giúp chuyển đổi các phản hồi "phi cấu trúc" của khách du lịch thành các thông tin "có cấu trúc" phục vụ ra quyết định.

### 2. Đánh giá Ưu và Nhược điểm

- **Ưu điểm**:
  - **Kiến trúc tiên tiến**: Sử dụng Lakehouse giúp tối ưu chi phí lưu trữ trong khi vẫn giữ được hiệu suất truy vấn cao.
  - **Tính tự động hóa cao**: Quy trình từ Ingestion đến Metadata Sync hoàn toàn khép kín.
  - **Cơ chế Fallback mạnh mẽ**: Hệ thống có khả năng tự phục hồi và duy trì tính liên tục của dữ liệu ngay cả khi API nguồn gặp sự cố nhờ kho dữ liệu Seed.
  - **Khả năng mở rộng**: Dễ dàng tích hợp thêm các nguồn dữ liệu mới (như TikTok, Facebook) mà không cần thay đổi kiến trúc lõi.
- **Nhược điểm**:
  - Khối lượng dịch vụ lớn đòi hỏi đội ngũ vận hành cần có kiến thức tổng hợp về Docker, Spark và SQL.
  - Cần tài nguyên máy chủ mạnh mẽ để duy trì các dịch vụ Real-time Governance.

### 3. Hướng mở rộng và Kiến nghị

- **Mở rộng**: Phát triển các Collector thu thập thêm văn bản đánh giá (Review Text) để triển khai các mô hình học máy phân tích cảm xúc (Sentiment Analysis) chuyên sâu.
- **Kiến nghị**: Đề nghị tích hợp hệ thống vào trục dữ liệu dùng chung của Thành phố để tạo ra một hệ sinh thái Du lịch Thông minh toàn diện.

---

## TÀI LIỆU THAM KHẢO

[1] M. Armbrust, A. Ghodsi, R. Xin, and M. Zaharia, "Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics," in *Proceedings of the 2021 Conference on Innovative Data Systems Research (CIDR)*, 2021.

[2] Databricks, "What is the Medallion Lakehouse Architecture?" [Online]. Available: <https://www.databricks.com/glossary/medallion-architecture>. [Accessed: 16-Apr-2026].

[3] Apache Iceberg Team, "Apache Iceberg Documentation & Netflix Origins," *iceberg.apache.org*. [Online]. Available: <https://iceberg.apache.org/docs/latest/>. [Accessed: 16-Apr-2026].

[4] OpenMetadata Community, "OpenLineage and Data Governance Integration Guide," *docs.open-metadata.org*. [Online]. Available: <https://docs.open-metadata.org/connectors/ingestion/openlineage>. [Accessed: 16-Apr-2026].

[5] Apache Airflow 3.1.0 Documentation, "TaskFlow API and dbt Cosmos Orchestration," *airflow.apache.org*.

---

## PHỤ LỤC

### Phụ lục A: Từ điển dữ liệu tầng Gold (Data Marts)

| Bảng | Cột | Kiểu dữ liệu | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- | :--- |
| `mart_district_stats` | `district` | VARCHAR | Tên quận/huyện được suy ra từ địa chỉ |
| | `total_attractions` | BIGINT | Tổng số điểm đến trong quận/huyện |
| | `avg_rating` | DOUBLE | Điểm đánh giá trung bình |
| | `total_reviews` | BIGINT | Tổng số lượt review |
| `mart_tourism_heatmap` | `name` | VARCHAR | Tên điểm đến |
| | `latitude`, `longitude` | DOUBLE | Tọa độ không gian (GPS) |
| | `popularity_score` | DOUBLE | Chỉ số phổ biến dùng cho heatmap |

### Phụ lục B: Bản đồ kết nối hạ tầng (Infrastructure Map)

- **Airflow UI**: `http://localhost:8080` (Điều phối Pipeline)
- **MinIO Console**: `http://localhost:9001` (Quản lý Data Lake)
- **Trino Engine**: `http://localhost:8888` (Cổng truy vấn SQL)
- **OpenMetadata**: `http://localhost:8585` (Data Catalog & Lineage)
- **Superset**: `http://localhost:8088` (Dashboard & Analytics)

### Phụ lục C: Minh họa logic hợp nhất dữ liệu (DataMerger Snippet)

```python
# Logic so khớp Fuzzy Matching tiêu biểu từ DataMerger
dist = geodesic((row['lat'], row['lon']), (other_row['lat'], other_row['lon'])).meters
name_sim = fuzz.token_sort_ratio(row['name'], other_row['name'])
if dist < self.distance_threshold or (dist < 300 and name_sim > self.name_threshold):
    current_cluster.append(other_row.to_dict())
```

### Phụ lục D: Cấu trúc thư mục dự án

- `apps/`: Frontend & Backend Management Portal.
- `infra/`: Cấu hình Docker Compose cho từng dịch vụ và các Spark Jobs (`infra/spark/jobs`).
- `data/`: Chứa dữ liệu Seed dùng cho cơ chế Fallback (tuyệt đối không xóa).
- `dbt_project/`: Thư mục riêng chứa các mô hình dbt (tầng Gold).
- `scripts/`: Các công cụ tiện ích (Health check, Khởi động hệ thống, Sinh Heatmap).
- `docs/`: Tài liệu dự án và báo cáo kết quả.
