from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv

load_dotenv()
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
AZURE_CONTAINER = "bronze"

def listar_arquivos_azure():
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER)
        
        print(f"📁 Listando arquivos no container: {AZURE_CONTAINER}\n")
        blobs = container_client.list_blobs()
        
        for blob in blobs:
            print(f"🔹 {blob.name} ({blob.size / 1024:.2f} KB)")
            
    except Exception as e:
        print(f"❌ Erro ao acessar Azure: {e}")

if __name__ == "__main__":
    listar_arquivos_azure()