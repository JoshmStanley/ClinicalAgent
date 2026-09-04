import logging


def configure_logging(level: str = "INFO", service: str = "service") -> None:
    logging.basicConfig(
        level=level.upper(),
        format=f"%(asctime)s %(levelname)s [{service}] %(name)s: %(message)s",
    )
