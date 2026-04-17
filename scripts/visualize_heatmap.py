import trino
import pandas as pd
import folium
from folium.plugins import HeatMap
import os

def generate_hanoi_heatmap():
    # 1. Ket noi den Trino
    conn = trino.dbapi.connect(
        host='localhost',
        port=8888, # Cong HTTP cua Trino
        user='admin',
        catalog='iceberg',
        schema='gold',
    )
    
    print("Fetching data from Gold layer...")
    query = """
        SELECT latitude AS lat, longitude AS lon, popularity_score AS avg_rating, name
        FROM mart_tourism_heatmap
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        print("No data found in Gold layer. Please run the enriched Silver/Gold pipeline first.")
        return

    # 2. Khoi tao ban do tai trung tam Ha Noi
    hanoi_map = folium.Map(location=[21.0285, 105.8542], zoom_start=13, tiles='OpenStreetMap')
    
    # 3. Chuan bi du lieu cho HeatMap (lat, lon, weight)
    # Trong so weight o day la avg_rating (Cang danh gia cao cang 'nong')
    heat_data = [[row['lat'], row['lon'], row['avg_rating']] for index, row in df.iterrows()]
    
    # 4. Them HeatMap layer
    HeatMap(heat_data, radius=15, blur=10, min_opacity=0.5).add_to(hanoi_map)
    
    # 5. Them cac Marker cho cac diem Top (Optional)
    for index, row in df.sort_values(by='avg_rating', ascending=False).head(10).iterrows():
        folium.Marker(
            [row['lat'], row['lon']], 
            popup=f"{row['name']} (Rating: {row['avg_rating']})",
            icon=folium.Icon(color='red', icon='info-sign')
        ).add_to(hanoi_map)

    # 6. Luu file
    output_path = 'hanoi_tourism_heatmap.html'
    hanoi_map.save(output_path)
    print(f"✅ Success! Heatmap saved to: {os.path.abspath(output_path)}")

if __name__ == "__main__":
    # Yeu cau cai dat: pip install trino pandas folium
    generate_hanoi_heatmap()
