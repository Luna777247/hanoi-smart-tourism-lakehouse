# File: infra/airflow/dags/libs/gold_quality_check.py
import great_expectations as gx
from trino.dbapi import connect
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def run_gold_quality_check(table_name="gold_attractions"):
    """
    Sử dụng Great Expectations để kiểm tra chất lượng dữ liệu tầng Gold.
    """
    logger.info(f"--- Bắt đầu kiểm tra chất lượng dữ liệu: {table_name} ---")
    
    # 1. Kết nối Trino
    try:
        conn = connect(
            host='lakehouse-trino',
            port=8080,
            user='admin',
            catalog='iceberg',
            schema='gold',
        )
        query = f"SELECT * FROM {table_name} LIMIT 1000"
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        logger.error(f"Lỗi kết nối Trino: {e}")
        return False

    if df.empty:
        logger.warning(f"Bảng {table_name} trống. Bỏ qua kiểm tra.")
        return True

    # 2. Khởi tạo Great Expectations
    context = gx.get_context()
    datasource = context.sources.add_pandas(name="gold_datasource")
    data_asset = datasource.add_dataframe_asset(name=table_name)
    
    batch = data_asset.build_batch_request(dataframe=df)
    
    # 3. Định nghĩa Expectations Suite
    suite_name = f"{table_name}_suite"
    suite = context.add_expectation_suite(expectation_suite_name=suite_name)
    
    validator = context.get_validator(
        batch_request=batch,
        expectation_suite_name=suite_name,
    )

    # --- CÁC KỊCH BẢN KIỂM TRA ---
    
    # Kiểm tra cột bắt buộc không được null
    validator.expect_column_values_to_not_be_null("osm_id")
    validator.expect_column_values_to_not_be_null("name")
    
    # Kiểm tra tính hợp lệ của tọa độ (Hà Nội nằm khoảng vĩ độ 20-21, kinh độ 105-106)
    validator.expect_column_values_to_be_between("latitude", min_value=20.0, max_value=22.0)
    validator.expect_column_values_to_be_between("longitude", min_value=105.0, max_value=107.0)
    
    # Kiểm tra Rating phải nằm trong khoảng 0-5
    if "rating" in df.columns:
        validator.expect_column_values_to_be_between("rating", min_value=0.0, max_value=5.0)

    # 4. Chạy kiểm tra
    results = validator.validate()
    
    if results.success:
        logger.info(f"✅ CHẤT LƯỢNG ĐẠT: Bảng {table_name} thỏa mãn tất cả các kịch bản kiểm tra.")
    else:
        logger.error(f"❌ CẢNH BÁO CHẤT LƯỢNG: Bảng {table_name} có dữ liệu không hợp lệ!")
        # Log chi tiết các lỗi
        for res in results.results:
            if not res.success:
                logger.error(f"  - Lỗi tại: {res.expectation_config.kwargs.get('column')} -> {res.expectation_config.expectation_type}")
    
    return results.success

if __name__ == "__main__":
    # Để test cục bộ nếu cần
    run_gold_quality_check()
