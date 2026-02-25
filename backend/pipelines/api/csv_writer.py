import csv
import io


def dict_to_csv_line(record: dict, columns: list) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([record.get(col, "") for col in columns])
    return buf.getvalue().strip()
