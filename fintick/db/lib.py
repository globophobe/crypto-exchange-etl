from .tables import MetatickTrade


def get_or_create_tables():
    if not MetatickTrade.exists():
        MetatickTrade.create_table()
