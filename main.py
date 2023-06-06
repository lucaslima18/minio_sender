#!/usr/bin/env python3

import os
import pathlib
from io import BytesIO
from urllib.parse import urlparse

import pandas as pd
from minio import Minio


class MinioSender():

    def __init__(self) -> None:
        self.url: str = ''
        self.access: str = ''
        self.secret: str = ''
        self.secure: bool = False

    def minio_client(self) -> Minio:
        client: Minio = Minio(
                self.url,
                access_key=self.access,
                secret_key=self.secret,
                secure=self.secure
            )

        return client

    def get_file_name(self, url) -> str:
        url_string = urlparse(url)
        filename: str = os.path.basename(url_string.path)
        return filename

    def send_csv_file(self, url_file) -> None:
        print(f'sending: {url_file}')
        client: Minio = self.minio_client()
        df = pd.read_csv(
                str(url_file),
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
                    'raw',
                    str(self.get_file_name(url_file)),
                    data=csv_buffer,
                    length=len(csv_bytes),
                    content_type='application/csv'
                )
        except Exception as exc:
            print(exc)

    def send_all_csv_files(self, url) -> None:
        folder_addr = pathlib.Path(url)
        for file in list(folder_addr.rglob('*csv')):
            self.send_csv_file(str(file))


if __name__ == '__main__':
    m = MinioSender()
    m.send_all_csv_files('')
