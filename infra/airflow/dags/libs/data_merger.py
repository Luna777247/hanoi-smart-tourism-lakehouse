# File: infra/airflow/dags/libs/data_merger.py
import logging
import pandas as pd
from geopy.distance import geodesic
from fuzzywuzzy import fuzz
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class DataMerger:
    """
    Module merge du lieu tu Google va OSM.
    Su dung fuzzy matching cho ten va geodistance cho toa do.
    """
    
    def __init__(self, distance_threshold_meters: float = 100, name_similarity_threshold: int = 80):
        self.distance_threshold = distance_threshold_meters
        self.name_threshold = name_similarity_threshold

    def _normalize_google(self, data: Dict) -> List[Dict]:
        normalized = []
        places = data.get("places", [])
        for p in places:
            normalized.append({
                "name": p.get("displayName", {}).get("text", ""),
                "lat": p.get("location", {}).get("latitude"),
                "lon": p.get("location", {}).get("longitude"),
                "rating": p.get("rating"),
                "source": "google",
                "category": p.get("types", [None])[0] if p.get("types") else "attraction"
            })
        return normalized

    def _normalize_osm(self, data: List[Dict]) -> List[Dict]:
        normalized = []
        for area_data in data:
            elements = area_data.get("elements", [])
            for el in elements:
                tags = el.get("tags", {})
                lat = el.get("lat") or el.get("center", {}).get("lat")
                lon = el.get("lon") or el.get("center", {}).get("lon")
                name = tags.get("name") or tags.get("name:vi") or tags.get("name:en")
                
                if name and lat and lon:
                    normalized.append({
                        "name": name,
                        "lat": lat,
                        "lon": lon,
                        "rating": None,
                        "source": "osm",
                        "category": tags.get("tourism") or tags.get("amenity") or tags.get("historic")
                    })
        return normalized
    def merge(self, google_data: Dict, osm_data: List[Dict]) -> pd.DataFrame:
        """
        Merge du lieu tu các nguồn.
        """
        g_list = self._normalize_google(google_data)
        o_list = self._normalize_osm(osm_data)
        
        all_places = g_list + o_list
            
        if not all_places:
            return pd.DataFrame()

        df = pd.DataFrame(all_places)
        merged_results = []
        
        used_indices = set()

        for i, row in df.iterrows():
            if i in used_indices:
                continue
            
            current_cluster = [row.to_dict()]
            used_indices.add(i)

            # Look for matches in the rest of the list
            for j, other_row in df.iterrows():
                if j in used_indices:
                    continue
                
                # Check distance
                dist = geodesic((row['lat'], row['lon']), (other_row['lat'], other_row['lon'])).meters
                
                # Check name similarity
                name_sim = fuzz.token_sort_ratio(row['name'], other_row['name'])
                
                if dist < self.distance_threshold or (dist < 300 and name_sim > self.name_threshold):
                    current_cluster.append(other_row.to_dict())
                    used_indices.add(j)
            
            # Merge the cluster into a single record
            merged_results.append(self._collapse_cluster(current_cluster))

        return pd.DataFrame(merged_results)

    def _collapse_cluster(self, cluster: List[Dict]) -> Dict:
        """Merge a cluster of similar locations into one."""
        sources = list(set(c['source'] for c in cluster))
        
        # Priority: Google > OSM (for name and location)
        primary = next((c for c in cluster if c['source'] == 'google'), cluster[0])
        
        # Get best rating
        ratings = [c['rating'] for c in cluster if c['rating'] is not None]
        avg_rating = sum(ratings) / len(ratings) if ratings else None
        
        return {
            "name": primary['name'],
            "lat": primary['lat'],
            "lon": primary['lon'],
            "rating": avg_rating,
            "sources": ",".join(sources),
            "category": primary['category'],
            "cluster_size": len(cluster)
        }
