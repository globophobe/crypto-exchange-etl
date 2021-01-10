import math


def calc_exponent(volume, divisor=10, decimal_places=1):
    if volume > 0:
        is_round = volume % math.pow(divisor, decimal_places) == 0
        if is_round:
            decimal_places += 1
            stop_execution = False
            while not stop_execution:
                is_round = volume % math.pow(divisor, decimal_places) == 0
                if is_round:
                    decimal_places += 1
                else:
                    stop_execution = True
            return decimal_places - 1
        else:
            return 0
    else:
        # WTF Bybit!
        return 0
