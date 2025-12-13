import json
from aind_data_access_api.document_db import MetadataDbClient

API_GATEWAY_HOST = "api.allenneuraldynamics.org"

docdb_api_client = MetadataDbClient(
    host=API_GATEWAY_HOST,
)

filter = {"subject.subject_id": "789917"}
count = docdb_api_client._count_records(
    filter_query=filter,
)
print(count)

