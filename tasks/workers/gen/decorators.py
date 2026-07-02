import logging
import time
import traceback
from functools import wraps

import requests


def send_exceptions_to_teams(webhook_url: str):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                payload = {
                    "type": "message",
                    "attachments": [
                        {
                            "contentType": "application/vnd.microsoft.card.adaptive",
                            "contentUrl": None,
                            "content": {
                                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                                "type": "AdaptiveCard",
                                "version":"1.2",
                                "body":[
                                    {
                                    "type": "TextBlock",
                                    "weight": "Bolder",
                                    "size": "ExtraLarge",
                                    "text": f"Error in {func.__name__}\n",
                                    "wrap": True
                                    },
                                    {
                                    "type": "TextBlock",
                                    "text": (
                                        f"**Type**: {type(e).__name__}\n"
                                        f"**Message**: {e}\n"
                                        f"**Traceback**:\n{traceback.format_exc()}"
                                    ),
                                    "wrap": True
                                    }
                                ]
                            }
                        }
                    ]
                    }
                try:
                    response = requests.post(webhook_url, json=payload, timeout=10)
                    response.raise_for_status()
                except Exception as message_e:
                    logging.warning(f"Failed to send a Teams message: {message_e}")
                raise
        return wrapper
    return decorator


def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            return func(*args, **kwargs)
        finally:
            logging.info(f"Execution time of {func.__name__}: {time.time() - start_time}sec")
    return wrapper
