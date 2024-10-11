import sys
import logging
from .driver import Driver
from driver_library_{{ org_name }}_{{ venture_name }}.driver_library.utils.core.environment import CheckEnvironment

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Constants:
    # per job parameters
    docker_variables = ["talkwalker_output", "output_template", "xcom_template", "query_hash", "project_id", "topic_id",
                        "from_date", "to_date", "project_name", "topic_name", "vendor_name", "source_format",
                        "venture_name"]

    # cloud parameters (from kubernetes secrets)
    secret_variables = ["TWITTER_TOKEN"]

    # configuration parameters
    configuration_variables = ["BUCKET_LOCATION"]


def run(args: dict) -> dict:
    """
    Main entry point into social feed driver called from Arflow DAG.
    :param args: args is a dictionary with all inputs needed to run the program
    :return: return value is a dictionary with oll outputs (s3 location in this case)
    """
    logger.info(f"Social Feed Driver - started.")

    logger.info(f'input args = {args}')

    if not CheckEnvironment.check_keys(Constants.docker_variables, args):
        logger.error(f"Required variables for social feed are not present.")
        sys.exit(1)

    env_vars = CheckEnvironment.get_env(Constants.secret_variables + Constants.configuration_variables)

    logger.info(f'env vars = {env_vars}')

    if not CheckEnvironment.check_keys(Constants.secret_variables + Constants.configuration_variables, env_vars):
        logger.error(f"Required environment variables for social feed are not present.")
        sys.exit(1)

    all_vars = {**args, **env_vars}

    driver: Driver = Driver()

    rc: dict = driver.run(all_vars)

    logger.info("Completed Social Feed Driver.")

    return rc