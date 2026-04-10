import logging

def initialise_logging(config_info):
    # logging level = DEBUG, INFO, WARNING, ERROR, CRITICAL 
    # filemode = append, write

    log_file = config_info["paths"]["output_log_file"]
    logging.basicConfig(
                        filename=log_file,
                        level=logging.INFO,
                        filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                        )
    
def log_message(msg,type="info"):
     logger = logging.getLogger()

     match type:
        case "debug":
            logger.debug(msg)
        case "info":
            logger.info(msg)
        case "warning":
            logger.warning(msg)
        case "error":
            logger.error(msg)
        case "critical":
            logger.critical(msg)
        case _:
            logger.info(msg)
