from prometheus_client import Counter, Gauge, Histogram

COMMANDS_SENT_TOTAL = Counter(
    "gateway_commands_sent_total",
    "Total number of commands sent to shuttles",
    ["shuttle_id", "command_type", "status"]
)
SHUTTLE_MESSAGES_RECEIVED_TOTAL = Counter(
    "gateway_shuttle_messages_received_total",
    "Total number of messages received from shuttles",
    ["shuttle_id", "message_type"]
)
SHUTTLE_ERRORS_TOTAL = Counter(
    "gateway_shuttle_errors_total",
    "Total number of F_CODE errors reported by shuttles",
    ["shuttle_id", "f_code"]
)
ACTIVE_SHUTTLE_CONNECTIONS = Gauge(
    "gateway_active_shuttle_connections",
    "Number of active TCP connections from shuttles"
)
SHUTTLE_BATTERY_LEVEL = Gauge(
    "gateway_shuttle_battery_level_percent",
    "Last reported battery level of a shuttle",
    ["shuttle_id"]
)
COMMAND_QUEUE_SIZE_METRIC = Gauge( # Переименовал, чтобы не конфликтовать с переменной очереди
    "gateway_command_queue_size",
    "Current size of the WMS command queue"
)