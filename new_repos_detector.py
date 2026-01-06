"""Detect new repositories by comparing current and previous snapshots."""
from __future__ import annotations

import sys
from pathlib import Path

import click
import pandas as pd
from rich import print

THIS_DIR = Path(__file__).parent


@click.command()
@click.option(
    "-p",
    "--previous",
    default="repos_previous.csv",
    help="Path to previous repos snapshot",
)
@click.option(
    "-c",
    "--current",
    default="repos.csv",
    help="Path to current repos snapshot",
)
def cli(previous: str, current: str) -> None:
    """Detect new repositories by comparing snapshots."""
    previous_path = THIS_DIR / previous
    current_path = THIS_DIR / current

    # Check if previous snapshot exists
    if not previous_path.exists():
        print(f"[yellow]No previous snapshot found at {previous_path}[/yellow]")
        print("[yellow]This appears to be the first run. No new repos to report.[/yellow]")
        sys.exit(0)

    # Load dataframes
    try:
        previous_df = pd.read_csv(previous_path)
        current_df = pd.read_csv(current_path)
    except Exception as e:
        print(f"[red]Error reading CSV files: {e}[/red]")
        sys.exit(1)

    # Find new repos (present in current but not in previous)
    new_repos = current_df[~current_df['full_name'].isin(previous_df['full_name'])]

    if len(new_repos) == 0:
        print("[green]No new repositories detected.[/green]")
        sys.exit(0)

    print(f"[green]Found {len(new_repos)} new repository(ies):[/green]")
    for _, repo in new_repos.iterrows():
        print(f"  • {repo['full_name']}")
        if pd.notna(repo['description']):
            print(f"    {repo['description']}")

    # Write new repos to a separate file for processing
    output_path = THIS_DIR / "new_repos.csv"
    new_repos.to_csv(output_path, index=False)
    print(f"\n[green]New repos saved to {output_path}[/green]")

    # Exit with code 1 to indicate new repos were found (for GitHub Actions)
    sys.exit(1)


if __name__ == "__main__":
    cli()
