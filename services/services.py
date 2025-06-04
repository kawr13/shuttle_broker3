from prometheus_client import Counter, Gauge

# Метрики для мониторинга шаттлов
COMMANDS_SENT_TOTAL = Counter(
    'shuttle_commands_sent_total',
    'Total number of commands sent to shuttles',
    ['shuttle_id', 'command_type', 'status']
)

SHUTTLE_MESSAGES_RECEIVED_TOTAL = Counter(
    'shuttle_messages_received_total',
    'Total number of messages received from shuttles',
    ['shuttle_id', 'message_type']
)

SHUTTLE_ERRORS_TOTAL = Counter(
    'shuttle_errors_total',
    'Total number of errors reported by shuttles',
    ['shuttle_id', 'f_code']
)

ACTIVE_SHUTTLE_CONNECTIONS = Gauge(
    'active_shuttle_connections',
    'Number of active shuttle connections'
)

SHUTTLE_BATTERY_LEVEL = Gauge(
    'shuttle_battery_level',
    'Battery level of shuttles in percentage',
    ['shuttle_id']
)

COMMAND_QUEUE_SIZE_METRIC = Gauge(
    'command_queue_size',
    'Size of the command queue'
)

# Новые метрики для мониторинга повторных попыток и восстановления соединений
COMMAND_RETRY_TOTAL = Counter(
    'command_retry_total',
    'Total number of command retries',
    ['shuttle_id', 'command_type', 'attempt']
)

RECONNECTION_ATTEMPTS_TOTAL = Counter(
    'reconnection_attempts_total',
    'Total number of reconnection attempts',
    ['shuttle_id', 'result']
)

SHUTTLE_HEARTBEAT_STATUS = Gauge(
    'shuttle_heartbeat_status',
    'Status of shuttle heartbeat (1=active, 0=inactive)',
    ['shuttle_id']
)

# Метрики для мониторинга WMS API
WMS_COMMANDS_PROCESSED = Counter(
    'wms_commands_processed_total',
    'Total number of commands processed from WMS',
    ['command_type', 'document_type', 'status']
)

WMS_STATUS_UPDATES = Counter(
    'wms_status_updates_total',
    'Total number of status updates sent to WMS',
    ['command_type', 'document_type', 'status']
)

WMS_API_ERRORS = Counter(
    'wms_api_errors_total',
    'Total number of errors when communicating with WMS API',
    ['endpoint', 'error_type']
)