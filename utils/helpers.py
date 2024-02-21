from utils.colors import colors
import datetime
import calendar


def fatal(message):
    assert type(message) == str
    print(colors.FAIL + message)
    exit(0)


def logException(exp):
    print(exp)
    exit(0)


def getTimings(num_days):
    today = datetime.datetime.now()
    end_day = today - datetime.timedelta(days=1)
    start_day = end_day - datetime.timedelta(days=num_days - 1)
    start_month = start_day.strftime("%m")
    start_year = start_day.strftime("%Y")
    end_year = end_day.strftime("%Y")
    end_month = end_day.strftime("%m")
    days_in_current_month = calendar.monthrange(int(end_year), int(end_month))[1]
    days_left_in_current_month = days_in_current_month - end_day.day

    if num_days <= days_in_current_month - days_left_in_current_month:
        today = datetime.datetime.now()
        endDay = today - datetime.timedelta(days=1)
        startDay = endDay - datetime.timedelta(days=num_days - 1)
        startMonth = startDay.strftime("%m")
        startYear = startDay.strftime("%Y")
        diff = (endDay.day - startDay.day) + 1
        return {
            "sw1": {
                "start_day": startDay.day,
                "target_month": startMonth,
                "end_day": endDay.day,
                "year": startYear,
                "day_diff": diff,
            }
        }
    else:
        today = datetime.datetime.now()
        current_date = today - datetime.timedelta(days=1)
        current_month = current_date.strftime("%m")
        previous_date = current_date - datetime.timedelta(days=num_days - 1)
        previous_month = previous_date.strftime("%m")
        previous_year = previous_date.strftime("%Y")
        current_year = current_date.strftime("%Y")
        prev_total_days = calendar.monthrange(int(current_year), int(previous_month))[1]
        prev_start_day = previous_date.day
        prev_end_day = prev_total_days
        current_start_day = current_date.day - current_date.day + 1
        current_end_day = current_date.day
        prev_day_diff = prev_end_day - prev_start_day + 1
        current_day_diff = current_end_day - current_start_day + 1
        return {
            "sw1": {
                "start_day": prev_start_day,
                "target_month": previous_month,
                "end_day": prev_end_day,
                "year": previous_year,
                "day_diff": prev_day_diff,
            },
            "sw2": {
                "start_day": current_start_day,
                "target_month": current_month,
                "end_day": current_end_day,
                "year": current_year,
                "day_diff": current_day_diff,
            },
        }
