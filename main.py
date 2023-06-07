#!/usr/bin/env python3

import os
import pathlib
from io import BytesIO
from urllib.parse import urlparse

import pandas as pd
from minio import Minio


class MinioSender():

    def __init__(self, url: str, access: str, secret: str) -> None:
        self.url: str = url
        self.access: str = access
        self.secret: str = secret
        self.secure: bool = False

    def minio_client(self) -> Minio:
        client: Minio = Minio(
                self.url,
                access_key=self.access,
                secret_key=self.secret,
                secure=self.secure
            )

        return client

    def get_file_name(self, file_path: str) -> str:
        filename: str = os.path.basename(urlparse(file_path).path)
        return filename

    def send_csv_file(self, file_path: str, bucket_name: str) -> None:
        print(f'sending: {file_path} to {bucket_name} bucket!')
        client: Minio = self.minio_client()
        df = pd.read_csv(
                filepath_or_buffer=str(file_path),
                encoding='iso-8859-1',
                sep=';',
                header=None,
                index_col=False,
                low_memory=False
            )
        csv_bytes = df.to_csv().encode('utf-8')
        csv_buffer = BytesIO(csv_bytes)

        try:
            client.put_object(
                    bucket_name=bucket_name,
                    object_name=str(self.get_file_name(file_path)),
                    data=csv_buffer,
                    length=len(csv_bytes),
                    content_type='application/csv'
                )
        except Exception as exc:
            print(exc)

    def send_all_csv_files(self, folder_path: str, bucket_name: str) -> None:
        folder_addr = pathlib.Path(folder_path)
        for file in list(folder_addr.rglob('*csv')):
            self.send_csv_file(file_path=str(file), bucket_name=bucket_name)


if __name__ == '__main__':
    minio_url = ''
    minio_access_token = ''
    minio_secret_token = ''
    sender = MinioSender(url=minio_url, access=minio_access_token, secret=minio_secret_token)
    
    folder_path: str = '' # write here the folder path to .csv files
    bucket_name: str = '' # wrote your bucket name here
    sender.send_all_csv_files(folder_path=folder_path, bucket_name=bucket_name)
