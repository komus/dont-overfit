from dotenv import load_dotenv
import os
import boto3

load_dotenv()


class Bucket:
    """ Managing to S3 bucket """

    def __init__(self, name: str,):
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_aws_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.region_name = os.getenv("AWS_REGION_NAME")
        self.bucket_name = name

    def _connect_to_client(self):
        """ Connect to s3 bucket client """
        return boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.secret_aws_access_key,
            region_name=self.region_name
        )

    def _connect_to_resource(self):
        """ Connect to s3 bucket resource """
        return boto3.resource(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.secret_aws_access_key,
            region_name=self.region_name
        )

    def get_file(self, file_name):
        folder_path = {file_name}
        return self._connect_to_resource().Object(self.bucket_name, folder_path).get()['Body']

    def add_file(self, file_path, name_in_bucket):
        path_in_bucket = name_in_bucket
        with open(file_path, 'rb') as data:
            self._connect_to_client().upload_fileobj(data, self.bucket_name, path_in_bucket)

    def copy_file(self, old_path,  new_path):
        """ Copy file in bucket """
        new_path = new_path
        old_path = old_path
        self._connect_to_resource().meta.client.copy({'Bucket': self.bucket_name, 'Key': old_path}, self.bucket_name, new_path)

    def delete_file(self, path):
        """ Delete file from bucket """
        path = path
        self._connect_to_resource().Object(self.bucket_name, path).delete()

    def moving_file(self, old_path,  new_path):
        """ Moving file in bucket """
        self.copy_file(old_path, new_path)
        self.delete_file(old_path)

