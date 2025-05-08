import os
from typing import List, Dict, Any, Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel


class ShuttleNetworkConfig(BaseModel):
    host: str
    command_port: int = 2000  # Порт, на котором шаттл слушает команды


class Settings(BaseSettings):
    PROJECT_NAME: str = os.getenv('PROJECT_NAME', 'shuttle')
    API_V1_STR: str = os.getenv('API_V1_STR', '/api/v1')

    GATEWAY_HOST: str = os.getenv('GATEWAY_HOST', 'localhost')
    GATEWAY_PORT: int = int(os.getenv('GATEWAY_PORT', '8000'))
    SHUTTLE_LISTENER_PORT: int = int(os.getenv('SHUTTLE_LISTENER_PORT', '5000'))

    TCP_CONNECT_TIMEOUT: float = float(os.getenv('TCP_CONNECT_TIMEOUT', '5.0'))
    TCP_READ_TIMEOUT: float = float(os.getenv('TCP_READ_TIMEOUT', '20.0'))  # Увеличил для ожидания данных от шаттла
    TCP_WRITE_TIMEOUT: float = float(os.getenv('TCP_WRITE_TIMEOUT', '5.0'))

    SHUTTLES_CONFIG: Dict[str, ShuttleNetworkConfig] = {
        "virtual_shuttle_1": {"host": "127.0.0.1", "command_port": 2000},
        "shuttle_2": {"host": "10.10.10.12", "command_port": 2000},
        "shuttle_3": {"host": "10.10.10.13", "command_port": 2000},
    }

    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')

    REDIS_HOST: str = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT: int = int(os.getenv('REDIS_PORT', '6379'))
    REDIS_DB: int = int(os.getenv('REDIS_DB', '0'))
    REDIS_PASSWORD: Optional[str] = os.getenv('REDIS_PASSWORD', None)

    COMMAND_QUEUE_MAX_SIZE: int = int(os.getenv('COMMAND_QUEUE_MAX_SIZE', '1000'))
    COMMAND_PROCESSOR_WORKERS: int = int(os.getenv('COMMAND_PROCESSOR_WORKERS', '1'))

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding='utf-8', extra='ignore')


settings = Settings()
