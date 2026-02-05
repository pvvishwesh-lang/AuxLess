import io
import apache_beam as beam
import csv


def dict_to_csv_line(record,columns):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([record.get(col, "") for col in columns])
    return output.getvalue().strip()
