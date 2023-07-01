from azure.storage.blob import BlobServiceClient
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-s", "--connectionstring", help="blob connection string", required=True)
parser.add_argument("-c", "--container", help="container name", required=True)
parser.add_argument("-p", "--prefix", help="blob prefix", required=True)
parser.add_argument("-y", "--year", help="year (YYYY)", required=True)
parser.add_argument("-m", "--month", help="month (MM)", required=True)
parser.add_argument("-f", "--filters", help="filters", required=True)
args = parser.parse_args()

data_filters = args.filters.split(',')

blob_service_client = BlobServiceClient.from_connection_string(args.connectionstring)
with blob_service_client:
  container_client = blob_service_client.get_container_client(container=args.container)
  with container_client:
     name_starts_with = args.prefix + "y=" + args.year + "/m=" + args.month
     blob_list = container_client.list_blobs(name_starts_with=name_starts_with)
     for index, blob in enumerate(blob_list):
      print("[%d] START %s" % (index+1, blob.name))

      with open(file="tmp.json", mode="wb") as download_file:
        download_file.write(container_client.download_blob(blob.name).readall())

      with open(file="tmp.json", mode="r") as f:
        lines = f.readlines()
        for line in lines:
          for filter in data_filters:
            if filter in line:
              print(lines)
