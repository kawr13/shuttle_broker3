import os
from pprint import pprint
from typing import Dict, Optional

import yaml
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel, Field
import redis.asyncio as redis
import json


class ShuttleNetworkConfig(BaseModel):
    host: str
    command_port: int = 2000


class Settings(BaseSettings):
    PROJECT_NAME: str = os.getenv('PROJECT_NAME', 'shuttle')
    API_V1_STR: str = os.getenv('API_V1_STR', '/api/v1')
    GATEWAY_HOST: str = os.getenv('GATEWAY_HOST', 'localhost')
    GATEWAY_PORT: int = int(os.getenv('GATEWAY_PORT', '8000'))
    SHUTTLE_LISTENER_PORT: int = int(os.getenv('SHUTTLE_LISTENER_PORT', '5000'))
    TCP_CONNECT_TIMEOUT: float = float(os.getenv('TCP_CONNECT_TIMEOUT', '5.0'))
    TCP_READ_TIMEOUT: float = float(os.getenv('TCP_READ_TIMEOUT', '20.0'))
    TCP_WRITE_TIMEOUT: float = float(os.getenv('TCP_WRITE_TIMEOUT', '5.0'))
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    REDIS_HOST: str = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT: int = int(os.getenv('REDIS_PORT', '6379'))
    REDIS_DB: int = int(os.getenv('REDIS_DB', '0'))
    REDIS_PASSWORD: Optional[str] = os.getenv('REDIS_PASSWORD', None)
    COMMAND_QUEUE_MAX_SIZE: int = int(os.getenv('COMMAND_QUEUE_MAX_SIZE', '1000'))
    COMMAND_PROCESSOR_WORKERS: int = int(os.getenv('COMMAND_PROCESSOR_WORKERS', '1'))
    WMS_WEBHOOK_URL: str = os.getenv('WMS_WEBHOOK_URL', '')
    # Настройки для интеграции с WMS API
    WMS_API_URL: str = os.getenv('WMS_API_URL', 'http://localhost:8080')
    WMS_API_USERNAME: str = os.getenv('WMS_API_USERNAME', '')
    WMS_API_PASSWORD: str = os.getenv('WMS_API_PASSWORD', '')
    WMS_POLL_INTERVAL: int = int(os.getenv('WMS_POLL_INTERVAL', '60'))  # в секундах
    WMS_INTEGRATION_ENABLED: bool = os.getenv('WMS_INTEGRATION_ENABLED', 'false').lower() == 'true'
    SHUTTLES_CONFIG: Dict[str, ShuttleNetworkConfig] = {}
    STOCK_TO_SHUTTLE: Dict[str, list[str]] = {}

    redis_client: Optional[redis.Redis] = Field(default=None, exclude=True)

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding='utf-8', extra='ignore')

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.redis_client = redis.Redis(
            host=self.REDIS_HOST,
            port=self.REDIS_PORT,
            db=self.REDIS_DB,
            password=self.REDIS_PASSWORD,
            decode_responses=True
        )

    async def load_config(self):
        """Загружает конфигурацию из Redis."""
        shuttles_data = await self.redis_client.get("shuttles_config")
        stock_data = await self.redis_client.get("stock_to_shuttle")
        with open("initial_config.yaml", "r") as f:
            config_data = yaml.safe_load(f)
        pprint(config_data)
        if shuttles_data:
            self.SHUTTLES_CONFIG = {
                k: ShuttleNetworkConfig(**v) for k, v in json.loads(shuttles_data).items()
            }
        else:
            self.SHUTTLES_CONFIG = {
                k: ShuttleNetworkConfig(**v)
                for k, v in config_data["shuttles_config"].items()
            }
            await self.save_config()  # Сохраняем в Redis

        if stock_data:
            self.STOCK_TO_SHUTTLE = json.loads(stock_data)
        else:
            self.STOCK_TO_SHUTTLE = config_data["stock_to_shuttle"]
            await self.save_config()
        pprint(self.SHUTTLES_CONFIG)
        pprint(self.STOCK_TO_SHUTTLE)


    async def save_config(self):
        """Сохраняет конфигурацию в Redis."""
        await self.redis_client.set(
            "shuttles_config",
            json.dumps({k: v.dict() for k, v in self.SHUTTLES_CONFIG.items()})
        )
        await self.redis_client.set("stock_to_shuttle", json.dumps(self.STOCK_TO_SHUTTLE))

    async def update_shuttle_stock(self, shuttle_id: str, new_stock: str):
        """Перемещает шаттл на новый склад и обновляет конфигурацию."""
        for stock, shuttles in self.STOCK_TO_SHUTTLE.items():
            if shuttle_id in shuttles:
                shuttles.remove(shuttle_id)
        if new_stock not in self.STOCK_TO_SHUTTLE:
            self.STOCK_TO_SHUTTLE[new_stock] = []
        self.STOCK_TO_SHUTTLE[new_stock].append(shuttle_id)
        await self.save_config()


settings = Settings()