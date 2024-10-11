import json
import boto3
import logging
import pandas as pd
import awswrangler as wr
from twitter_{{ org_name }}_{{ solution_name }}.twitter.source import TwitterSource

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Constants:
    APPLICATION_NAME = "socialfeed"
    TEMP_DIR = "temp"


class Driver:
    """
    Driver class implements the social feed pull logic
    """

    def __init__(self):
        logger.info("=== Social Feed driver initialized. ===")

    @staticmethod
    def consolidate_jsonl(temp_path, output_file, curr_session):
        input_file = temp_path + f'/*.jsonl'
        df_read = wr.s3.read_json(input_file, lines=True, boto3_session=curr_session)
        logger.info("Record count: {}".format(len(df_read.index)))
        wr.s3.to_json(df_read, output_file, orient="records", lines=True, boto3_session=curr_session)
        logger.info(f">>> Consolidated data at: {output_file} ")
        return output_file

    @staticmethod
    def get_tweets_history(user_id, temp_path, curr_session, twitter_obj):
        out_file = f'{temp_path}/tweet_data_for_{user_id}.jsonl'
        data, err_count = TwitterSource.get_tweets_by_user_ids(twitter_obj, user_id)
        df_tweet_data = pd.DataFrame(data)
        wr.s3.to_json(df_tweet_data, out_file, orient="records", lines=True, boto3_session=curr_session)

    @staticmethod
    def initialize_boto3_session():
        try:
            boto3_session = boto3.Session()
            return boto3_session
        except Exception as e:
            logger.error("boto3 session could not be created")
            logger.exception(e)

    @staticmethod
    def parse_s3_filename(s3_uri):
        """
        Parses an S3 filename and extracts the bucket name, path, and path without the last filename portion.

        Args:
            s3_uri: The S3 filename string (e.g., s3://bucket-name/path/to/file.txt)

        Returns:
            A tuple containing three elements:
                - bucket_name: The name of the S3 bucket (str)
                - path: The path within the bucket, including sub folders (str)
                - path_without_filename: The path within the bucket excluding the last filename (str)

        Raises:
            ValueError: If the provided string is not a valid S3 URI.
        """

        if not s3_uri.startswith("s3://"):
            raise ValueError("Invalid S3 URI. Must start with 's3://'")

        parts = s3_uri[5:].split("/")

        if not parts:
            raise ValueError("Invalid S3 URI. Missing bucket name")

        bucket_name = parts[0]
        path = "/" + "/".join(parts[1:])

        # Extract path without filename (assuming filename has an extension)
        if "." in path:
            path_without_filename = path[:path.rfind("/")]
        else:
            path_without_filename = path

        return bucket_name, path, path_without_filename

    def run(self, args: dict) -> dict:
        """
        Main method in Driver class that invokes the entire logic of social feed
        :param args: args is a dictionary with all inputs needed to run the program
        :return: return value is a dictionary with oll outputs (s3 location in this case)
        """

        logger.setLevel(logging.INFO)

        logger.info(f'combined args = {args}')

        input_file = args["talkwalker_output"]
        bearer_token = args["TWITTER_TOKEN"]
        file_template = args["output_template"]
        query_hash = args["query_hash"]
        output_bucket = args["BUCKET_LOCATION"]

        # note :    s3 object key  = raw/{application}/{hash_id}/{from_date}_{to_date}/file_{int}.jsonl

        output_file_key = file_template.format(Constants.APPLICATION_NAME)
        output_file = f"s3://{output_bucket}/{output_file_key}"

        _, _, path_prefix = Driver.parse_s3_filename(output_file)
        temp_path = f"s3://{output_bucket}{path_prefix}/{Constants.TEMP_DIR}"

        logger.info(f'input file = {input_file}')
        logger.info(f'output file key = {output_file_key}')
        logger.info(f'output file = {output_file}')
        logger.info(f'temp path = {temp_path}')

        logger.info(">> validating aws credentials... ")
        curr_session = self.initialize_boto3_session()

        df_read = wr.s3.read_json(input_file, lines=True, boto3_session=curr_session)

        logger.info(">> Data count : {}".format(len(df_read.index)))

        df_twitter_filter = df_read.loc[df_read['external_provider'] == 'twitter']

        if len(df_twitter_filter.index) > 0:
            df_filter = df_twitter_filter[df_twitter_filter['author_id'].notnull()]
    
            logger.info(">> [Filtered] Data count : {}".format(len(df_filter.index)))
            logger.info(">> [Distinct] User count : {}".format(df_filter['author_id'].nunique()))
    
            distinct_users = df_filter['author_id'].unique().astype(int)
            df_users = pd.DataFrame({"author_id": distinct_users})
            twitter_obj = TwitterSource(None, None, bearer_token)
    
            df_users.apply(lambda row: Driver.get_tweets_history(
                row['author_id'], temp_path, curr_session, twitter_obj),
                           axis=1)
    
            Driver.consolidate_jsonl(temp_path, output_file, curr_session)
    
            logger.info(
                f"[SocialFeed - job for collecting tweets' history] is now completed with s3 location: {output_file}!!")
    
            print("_______________________________________________________________________________________________")
    
            data = {
                #"output_template": file_template,
                "socialfeed_output": output_file,
                "query_hash": query_hash,
                "project_id": args['project_id'],
                "topic_id": args['topic_id'],
                "from_date": args['from_date'],
                "to_date": args['to_date'],
                "project_name": args["project_name"],
                "topic_name": args["topic_name"],
                "vendor_name": "socialfeed",
                "source_format": args["source_format"],
                "solution_name": args["solution_name"],
            }
    
            logger.info(f'output = {data}')
            xcom_file_key = args["xcom_template"].format(Constants.APPLICATION_NAME)
            Driver.write_xcom_to_s3(curr_session, data, output_bucket, xcom_file_key)
            return data
        else:
            logger.info("Talkwalker dataset does not contain records with Twitter source to run Social Feed module")

    @staticmethod
    def write_xcom_to_s3(boto3_session, data_dict: dict, bucket_name: str, key: str):

        s3 = boto3_session.client('s3')

        # Serialize the dictionary to JSON
        json_data = json.dumps(data_dict).encode('utf-8')

        # Upload the JSON data to S3
        s3.put_object(Body=json_data, Bucket=bucket_name, Key=key)
