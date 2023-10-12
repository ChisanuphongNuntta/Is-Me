#Import packages
from azure.storage.blob import ContainerClient
from io import StringIO
import pandas as pd
#Create connection string
conn_str = "DefaultEndpointsProtocol=https;AccountName=heartstroage;AccountKey=nSDK6Iqa3pf51eke42wpu7hPnEGD3HJmzF7tnRHa5K+8Zz86685w7mwL8/jPnFVUMLcwqPvRg7Tk+AStmlOD3Q==;EndpointSuffix=core.windows.net"

#Declare container name
container_name = "fblivethailand"
#Declare Blob name
blob_name = "fb_live_thailand.csv"
#Create container client
container_client = ContainerClient.from_connection_string(conn_str,container_name)

#Create download blob
downloaded_blob = container_client.download_blob(blob_name,encoding='utf8')

#Pandas read and print dataframe
read_csv = pd.read_csv(StringIO(downloaded_blob.readall()),
low_memory=False)
df = pd.DataFrame(read_csv)
print(df)