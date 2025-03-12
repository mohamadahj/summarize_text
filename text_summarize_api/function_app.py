import azure.functions as func
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
import os
import json

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="summaries", methods=["GET"])
def get_summaries(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn_str = os.getenv("STORAGE_CONNECTION_STRING")
        container_name = "gold-layer"
        folder_name = "gold-summarized/"

        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        container_client = blob_service_client.get_container_client(container_name)

        blobs = container_client.list_blobs(name_starts_with=folder_name)
        dfs = []

        for blob in blobs:
            if blob.name.endswith(".parquet"):
                stream = io.BytesIO()
                container_client.download_blob(blob).readinto(stream)
                stream.seek(0)
                df = pd.read_parquet(stream)
                dfs.append(df)

        if dfs:
            final_df = pd.concat(dfs, ignore_index=True)
            result = final_df.to_dict(orient='records')
            return func.HttpResponse(
                json.dumps(result, indent=2),
                mimetype="application/json",
                status_code=200
            )
        else:
            return func.HttpResponse(
                json.dumps({"message": "No summarized data found."}),
                mimetype="application/json",
                status_code=404
            )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )
