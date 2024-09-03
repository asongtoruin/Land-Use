import logging
from pathlib import Path


def configure_logger(output_dir: Path, log_name: str) -> logging.Logger:
    """Sets up standard logging for Land-Use

    Parameters
    ----------
    output_dir : Path
        path to the output directory to be used
    log_name : str
        name to assign to the log file(s)

    Returns
    -------
    logging.Logger
        Logger object to use for generating any new logs
    """
    # Get the module logger
    lu_logger = logging.getLogger('land_use')
    cc_logger = logging.getLogger('caf.core')

    # Set up nice formatting
    log_formatter = logging.Formatter(
        fmt='[%(asctime)-15s] %(levelname)-7s - [%(name)s::%(funcName)s(#%(lineno)d)]: %(message)s',
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Configure both Land-Use and caf.core loggers:
    for logger in (lu_logger, cc_logger):
        # Set up logging - needs to be at the "most detailed" that we need
        logger.setLevel(logging.DEBUG)

        # Set up three different loggers:
        # - stream for reporting to screen
        # - "standard" output log file
        # - "detailed" output log file, including reporting at every stage
        sh = logging.StreamHandler()
        fh = logging.FileHandler(output_dir / f'{log_name}_{logger.name}.log', mode='w')
        detailed_logs = logging.FileHandler(output_dir / f'{log_name}_{logger.name}_detailed.log', mode='w')

        # Stream and standard need level setting
        for handler in (sh, fh):
            handler.setFormatter(log_formatter)
            handler.setLevel(logging.INFO)
            logger.addHandler(handler)

        # detailed_logs.setLevel(logging.DEBUG)
        detailed_logs.setFormatter(log_formatter)
        logger.addHandler(detailed_logs)

    # We want to capture warnings for the logs too!
    logging.captureWarnings(True)

    return lu_logger
