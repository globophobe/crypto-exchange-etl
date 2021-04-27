from .fscache import FSCache


class FinTickChecker(FSCache):
    """
    Need assertion both forwards and backwards
    """

    def assert_data_frame(self, data_frame):
        first_trade = data_frame.iloc[0]
        # Coerce to int b/c dataframe is int64, but firestore data is int32
        first_index = int(first_trade["index"])
        # Last partition
        data = self.firestore_cache.get_one(
            where=["close.index", "==", first_index - 1]
        )
        if data:
            # Is current partition contiguous with last partition?
            assert data["close"]["index"] == first_index - 1
            assert data["close"]["timestamp"] < first_trade["timestamp"]
