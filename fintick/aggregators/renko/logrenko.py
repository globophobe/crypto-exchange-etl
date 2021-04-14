from ...bqloader import stringify_datetime_types
from ...fscache import firestore_data
from .lib import aggregate_renko, get_initial_cache, get_log_level, get_value_display
from .renko import Renko


class LogRenko(Renko):
    def get_destination(self, sep="_"):
        basename = self.get_basename(sep=sep)
        box_size_display = get_value_display(self.box_size)
        return f"{basename}{sep}log{sep}renko{sep}{box_size_display}"

    def get_initial_cache(self, data_frame):
        return get_initial_cache(data_frame, self.box_size, level_func=get_log_level)

    def process_data_frame(self, data_frame):
        data_frame, cache = self.get_cache(data_frame)
        data, cache = aggregate_renko(
            data_frame, cache, self.box_size, top_n=self.top_n, level_func=get_log_level
        )
        # Index
        for index, d in enumerate(data):
            data[index] = stringify_datetime_types(firestore_data(d, strip_date=False))
            data[index]["topN"] = [
                stringify_datetime_types(firestore_data(t)) for t in d["topN"]
            ]
            data[index]["index"] = index
        return data, cache
