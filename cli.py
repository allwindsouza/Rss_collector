import csv
import typer

app = typer.Typer()

@app.command()
def add_url(url: str, csv_file: str):
    """Add a new row to the CSV file."""
    with open('data.csv', 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([name, age])
    typer.echo(f"Added {name}, {age} to CSV file.")


def count_csv_lines(file_path):
    """Count the number of lines in a CSV file."""
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        line_count = sum(1 for row in reader)
    return line_count


@app.command("add-pub-url")
def add_pub_url(
        url: str = typer.Option(
            ...,
            "--url",
            "-u",
            help="Publisher rss",
        ),
        csv_file: str = typer.Option(
            "/home/allwind/Desktop/CAS/Rss_collector/temp_rss_feed.csv",
            "--csv",
            "-c",
            help="path to csv file",
        ),
):
    id = count_csv_lines(csv_file)
    id += 3
    with open(csv_file, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([id, url, "null", "null", 0])

    print(f"Completed adding to csv at {csv_file}")

if __name__ == "__main__":
    app()