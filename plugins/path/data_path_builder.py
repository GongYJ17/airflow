from pathlib import Path
import pandas as pd
from common import config


class DataPathBuilder:
    """
    ingest_type(init / incremental)에 따라
    raw/tmp/staging/processed 경로를 생성하는 빌더
    """

    def __init__(self, ingest_type: str, base_dir: str = '/share/airflow/data'):
        self.base_dir = Path(base_dir)
        self.ingest_type = ingest_type

        if ingest_type == "init":
            self.root = self.base_dir / "init"
        elif ingest_type == "incremental":
            self.root = self.base_dir
        else:
            raise ValueError(f"Invalid ingest_type: {ingest_type}")

        # ✅ CSV 한 번 읽어서 허용 collection 목록 생성
        # df = pd.read_csv(config.category_id_map_path)

        # if "collection_nm" not in df.columns:
        #     raise ValueError("category_id_map.csv must contain 'collection_nm' column")

        # self.allowed_collections = set(df["collection_nm"].dropna())

    # def _check_collection(self, collection_nm: str):
    #     if collection_nm not in self.allowed_collections:
    #         raise ValueError(
    #             f"Invalid collection_nm: '{collection_nm}'. "
    #             f"Allowed values: {sorted(self.allowed_collections)}"
    #         )

    def raw(self, collection_nm: str, ts: str) -> Path:
        # self._check_collection(collection_nm)
        return self.root / "raw" / collection_nm / ts

    def tmp(self, collection_nm: str, ts: str) -> Path:
        # self._check_collection(collection_nm)
        return self.root / "tmp" / collection_nm / ts

    def staging(self, collection_nm: str, ts: str) -> Path:
        # self._check_collection(collection_nm)
        return self.root / "staging" / collection_nm / ts

    def processed(self, collection_nm: str, ts: str) -> Path:
        # self._check_collection(collection_nm)
        return self.root / "processed" / collection_nm / ts
