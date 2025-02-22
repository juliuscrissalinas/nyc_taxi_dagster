from dagster import ScheduleDefinition
from ..jobs import trip_update_job, weekly_update_job

trip_update_schedule = ScheduleDefinition(
    name="trip_update_schedule", cron_schedule="17 16 * * *", job=trip_update_job,
    execution_timezone="Asia/Singapore"  # UTC+8
)

weekly_update_schedule = ScheduleDefinition(
    name="weekly_update_schedule",
    cron_schedule="0 0 * * 1",  # every Tuesday at midnight
    job=weekly_update_job,
    execution_timezone="Asia/Singapore"  # UTC+8
)
