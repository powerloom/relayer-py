from settings.conf import settings


def tx_launcher_core_start_timestamp():
    return f'tx_launcher_core_start_timestamp:{settings.protocol_state_address.lower()}'


def timeslot_preference(slot_id):
    return f'timeslot_preference:{slot_id}'
