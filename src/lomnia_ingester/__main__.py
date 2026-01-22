# import logging
#
# from lomnia_ingester.config import load_config
# from lomnia_ingester.core.plugins.scheduler import schedule_and_wait
# from lomnia_ingester.logging import setup_logging
#
# setup_logging(level="DEBUG")
#
# logger = logging.getLogger(__name__)
# logger.info("Application starting")
#
# logger.info("Loading config")
#
#
# if __name__ == "__main__":
#     config = load_config()
#     storage = S3Storage(
#         bucket=config.s3.s3_bucket_name,
#         endpoint_url=config.s3.s3_url,
#         region_name=config.s3.s3_region_name,
#         access_key_id=config.s3.s3_access_key_id,
#         secret_access_key=config.s3.s3_secret_access_key,
#     )
#
#     queuePublisher = QueuePublisher(
#         host=config.queue.queue_host,
#         port=config.queue.queue_port,
#         username=config.queue.queue_username,
#         password=config.queue.queue_password,
#         queue_name=config.queue.queue_name,
#     )
#
#     publisher = PluginOutputPublisher(storage, queuePublisher)
#
#     store = PluginStateStore(config.store.store_path)
#     schedule_and_wait()
