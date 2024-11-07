from settings.conf import settings


def tx_launcher_core_start_timestamp():
    """
    Generate a Redis key for storing the start timestamp of the transaction launcher core.

    Returns:
        str: A Redis key string incorporating the protocol state address.
    """
    return f'tx_launcher_core_start_timestamp:{settings.protocol_state_address.lower()}'


def epoch_batch_submissions(epoch_id):
    """
    Generate a Redis key for storing batch submissions for a specific epoch.

    Args:
        epoch_id (int): The ID of the epoch.

    Returns:
        str: A Redis key string for epoch batch submissions.
    """
    return f'epoch_batch_submissions:{epoch_id}'


def epoch_batch_size(epoch_id):
    """
    Generate a Redis key for storing the batch size of a specific epoch.

    Args:
        epoch_id (int): The ID of the epoch.

    Returns:
        str: A Redis key string for epoch batch size.
    """
    return f'epoch_batch_size:{epoch_id}'


def timeslot_preference(slot_id):
    """
    Generate a Redis key for storing preferences for a specific time slot.

    Args:
        slot_id (int): The ID of the time slot.

    Returns:
        str: A Redis key string for time slot preferences.
    """
    return f'timeslot_preference:{slot_id}'


def end_batch_submission_called(data_market, epoch_id):
    """
    Generate a Redis key for storing whether the end batch submission has been called for a specific epoch
    and data market.
    """
    return f'end_batch_submission_called:{data_market}:{epoch_id}'
