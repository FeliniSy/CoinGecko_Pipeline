import json

from google.cloud import storage

from utils.logger import log
from utils.settings import GCS_BUCKET


class GCSClient:
    def __init__(self):
        self.client = storage.Client()
        self.bucket = self.client.bucket(GCS_BUCKET)

    def upload_to_gcs(self, all_coins, date_str, hour_str, execution_dt):
        blob_path = f"crypto/raw/date={date_str}/hour={hour_str}/markets_{execution_dt.strftime('%Y%m%d_%H%M%S')}.json"

        payload = {
            "fetched_at": execution_dt.isoformat(),
            "record_count": len(all_coins),
            "coins": all_coins,
        }

        blob = self.bucket.blob(blob_path)
        blob.upload_from_string(json.dumps(payload, ensure_ascii=False), content_type="application/json")
        log.info("Uploaded %s records → gs://%s/%s", len(all_coins), GCS_BUCKET, blob_path)

        return blob_path


gsc_client = GCSClient()