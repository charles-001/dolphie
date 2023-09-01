from decimal import Decimal

import charset_normalizer


def format_bytes(bytes_value, color=True):
    units = ["B", "KB", "MB", "GB", "TB"]
    unit_index = 0

    while bytes_value >= 1024 and unit_index < len(units) - 1:
        bytes_value /= 1024
        unit_index += 1

    formatted_value = f"{bytes_value:.2f}"

    if formatted_value.endswith(".00"):
        formatted_value = formatted_value[:-3]  # Remove ".00" from the end

    if color:
        return f"{formatted_value}[highlight]{units[unit_index]}[/highlight]"
    else:
        return f"{formatted_value}{units[unit_index]}"


def format_time(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def detect_encoding(text):
    # Since BLOB/BINARY data can be involved, we need to auto-detect what the encoding is
    # for queries since it can be anything. If I let pymsql use unicode by default I got
    # consistent crashes due to unicode errors for utf8 so we have to go this route
    result = charset_normalizer.detect(text)
    encoding = result["encoding"]

    if encoding is None:
        encoding = "latin1"
    elif encoding == "utf-16be":
        encoding = "utf-8"

    return encoding


def round_num(n, decimal=2):
    n = Decimal(n)
    return n.to_integral() if n == n.to_integral() else round(n.normalize(), decimal)


# This is from https://pypi.org/project/numerize
def format_number(n, decimal=2, color=True):
    if not n:
        return "0"

    # fmt: off
    sufixes = ["", "K", "M", "B", "T", "Qa", "Qu", "S", "Oc", "No",
               "D", "Ud", "Dd", "Td", "Qt", "Qi", "Se", "Od", "Nd", "V",
               "Uv", "Dv", "Tv", "Qv", "Qx", "Sx", "Ox", "Nx", "Tn", "Qa",
               "Qu", "S", "Oc", "No", "D", "Ud", "Dd", "Td", "Qt", "Qi",
               "Se", "Od", "Nd", "V", "Uv", "Dv", "Tv", "Qv", "Qx", "Sx",
               "Ox", "Nx", "Tn", "x", "xx", "xxx", "X", "XX", "XXX", "END"]

    sci_expr = [1e0, 1e3, 1e6, 1e9, 1e12, 1e15, 1e18, 1e21, 1e24, 1e27,
                1e30, 1e33, 1e36, 1e39, 1e42, 1e45, 1e48, 1e51, 1e54, 1e57,
                1e60, 1e63, 1e66, 1e69, 1e72, 1e75, 1e78, 1e81, 1e84, 1e87,
                1e90, 1e93, 1e96, 1e99, 1e102, 1e105, 1e108, 1e111, 1e114, 1e117,
                1e120, 1e123, 1e126, 1e129, 1e132, 1e135, 1e138, 1e141, 1e144, 1e147,
                1e150, 1e153, 1e156, 1e159, 1e162, 1e165, 1e168, 1e171, 1e174, 1e177]
    # fmt: on

    # Convert string to a number format if needed
    if isinstance(n, str):
        try:
            n = float(n)
        except ValueError:
            return n

    n = abs(n)
    for x in range(len(sci_expr)):
        if n >= sci_expr[x] and n < sci_expr[x + 1]:
            sufix = sufixes[x]
            if n >= 1e3:
                num = str(round_num(n / sci_expr[x], decimal))
            else:
                num = str(round_num(n, 0))
            if color:
                return f"{num}[highlight]{sufix}[/highlight]" if sufix else num
            else:
                return f"{num}{sufix}" if sufix else num


def format_sys_table_memory(data):
    parsed_data = data.strip().split(" ")
    if len(parsed_data) == 2:
        value, suffix = parsed_data[0], parsed_data[1][:1]

        if value == "0":
            suffix = ""
        elif suffix != "b":
            suffix += "B"
        elif suffix == "b":
            suffix = "B"

        return f"{value}[highlight]{suffix}"

    return data
