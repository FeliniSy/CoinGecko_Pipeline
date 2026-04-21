import json

from google.cloud import storage

from utils.logger import log
from utils.settings import GCS_BUCKET, GCS_BASE_PATH


class GCSClient:
    def __init__(self):
        self.client = storage.Client()
        self.bucket = self.client.bucket(GCS_BUCKET)

    def upload_to_gcs(self, all_coins, date_str, hour_str, execution_dt):
        blob_path = f"{GCS_BASE_PATH}/date={date_str}/hour={hour_str}/markets_{execution_dt.strftime('%Y%m%d_%H%M%S')}.json"

        payload = {
            "fetched_at": execution_dt.isoformat(),
            "record_count": len(all_coins),
            "coins": all_coins,
        }

        blob = self.bucket.blob(blob_path)
        blob.upload_from_string(json.dumps(payload, ensure_ascii=False), content_type="application/json")
        log.info("Uploaded %s records → gs://%s/%s", len(all_coins), GCS_BUCKET, blob_path)

        return blob_path

    def upload_json(self, blob_path: str, data: dict, timeout: int = 60) -> str:
        try:
            blob = self.bucket.blob(blob_path)
            blob.upload_from_string(
                json.dumps(data, ensure_ascii=False),
                content_type="application/json",
                timeout=timeout
            )
            gcs_uri = f"gs://{GCS_BUCKET}/{blob_path}"
            log.info("✓ Uploaded to %s", gcs_uri)
            return gcs_uri

        except Exception as e:
            log.error("Failed to upload to gs://%s/%s: %s", GCS_BUCKET, blob_path, str(e))
            raise

    def blob_exists(self, blob_path: str, timeout: int = 10) -> bool:
        try:
            blob = self.bucket.blob(blob_path)
            return blob.exists(timeout=timeout)
        except Exception as e:
            log.warning("Could not check existence for %s: %s", blob_path, str(e))
            return False


gsc_client = GCSClient()