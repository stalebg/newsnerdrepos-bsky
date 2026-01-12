"""Post new repositories to Bluesky."""
from __future__ import annotations

import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone

import click
import deepl
import pandas as pd
from atproto import Client
from rich import print

THIS_DIR = Path(__file__).parent

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def translate_to_norwegian(text: str) -> str:
    """
    Translate text to Norwegian using DeepL API.
    Falls back to original text if translation fails.
    """
    if not text or not text.strip():
        return text

    api_key = os.getenv("DEEPL_API_KEY")
    if not api_key:
        logger.warning("DEEPL_API_KEY not set, skipping translation")
        return text

    try:
        translator = deepl.Translator(api_key)
        result = translator.translate_text(text, target_lang="NB")  # Norwegian Bokmål
        return result.text
    except Exception as e:
        logger.warning(f"Translation failed: {e}")
        return text  # Return original on failure


def authenticate_bluesky(username: str, password: str) -> Client:
    """
    Authenticate to Bluesky and return an authenticated client.

    Args:
        username: Bluesky username/handle
        password: Bluesky app password

    Returns:
        Authenticated Bluesky client
    """
    client = Client()
    try:
        # Perform login
        response = client.login(username, password)
        logger.debug(f"Login response: {response}")

        # Attempt to extract DID from the login response if available
        if isinstance(response, dict) and 'did' in response:
            client.did = response['did']
        elif hasattr(response, 'did'):
            client.did = response.did
        else:
            # If DID not available in the login response, resolve the handle
            logger.info(f"Resolving DID for handle: {username}")
            try:
                handle_response = client.com.atproto.identity.resolve_handle(username)
                if isinstance(handle_response, dict) and 'did' in handle_response:
                    client.did = handle_response['did']
                    logger.info(f"Successfully resolved DID: {client.did}")
                else:
                    logger.error("Failed to resolve DID from handle response.")
                    raise SystemExit("Unable to proceed without DID information.")
            except Exception as resolve_e:
                logger.error(f"Failed to resolve handle to DID: {resolve_e}")
                raise SystemExit("Unable to proceed without DID information.")

        logger.info("Successfully authenticated to Bluesky API.")
        return client
    except Exception as e:
        logger.error(f"Failed to authenticate to Bluesky API: {e}")
        raise SystemExit("Unable to proceed without Bluesky authentication.")


def create_repo_post(repo: dict) -> dict:
    """
    Create a Bluesky post record for a new repository.

    Args:
        repo: Dictionary containing repository information

    Returns:
        Post record ready to be posted to Bluesky
    """
    org = repo['org']
    repo_name = repo['name']
    full_name = repo['full_name']
    repo_url = f"https://github.com/{full_name}"
    description = repo.get('description', '')

    # Translate description to Norwegian
    if pd.notna(description) and description:
        description = translate_to_norwegian(description)

    # Construct the post text (Norwegian)
    if pd.notna(description) and description:
        text = f"{org} har nettopp åpnet repoet {full_name}: {repo_url}\n\n{description}"
    else:
        text = f"{org} har nettopp åpnet repoet {full_name}: {repo_url}"

    # Truncate if too long (Bluesky has a 300 character limit)
    if len(text) > 300:
        # Calculate how much space we have for description
        base_text = f"{org} har nettopp åpnet repoet {full_name}: {repo_url}\n\n"
        remaining = 300 - len(base_text) - 3  # -3 for "..."
        if remaining > 0 and pd.notna(description):
            text = base_text + description[:remaining] + "..."
        else:
            text = f"{org} har nettopp åpnet repoet {full_name}: {repo_url}"

    # Encode the text to UTF-8 to properly calculate byte positions
    text_bytes = text.encode('utf-8')
    url_bytes = repo_url.encode('utf-8')

    # Find the byte positions of the URL in the UTF-8 encoded text
    url_start = text_bytes.find(url_bytes)
    url_end = url_start + len(url_bytes)

    # Create facets to make the URL clickable
    facets = [
        {
            "index": {
                "byteStart": url_start,
                "byteEnd": url_end
            },
            "features": [{
                "$type": "app.bsky.richtext.facet#link",
                "uri": repo_url
            }]
        }
    ]

    # Create the post record
    record = {
        "$type": "app.bsky.feed.post",
        "text": text,
        "facets": facets,
        "createdAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }

    return record


def post_to_bluesky(client: Client, record: dict) -> bool:
    """
    Post a record to Bluesky.

    Args:
        client: Authenticated Bluesky client
        record: Post record to publish

    Returns:
        True if successful, False otherwise
    """
    try:
        if not hasattr(client, 'did') or not client.did:
            logger.error("Client object does not have valid DID information.")
            return False

        response = client.com.atproto.repo.create_record(
            data={
                "collection": "app.bsky.feed.post",
                "repo": client.did,
                "record": record
            }
        )

        if hasattr(response, 'uri') or (isinstance(response, dict) and 'uri' in response):
            post_uri = response.uri if hasattr(response, 'uri') else response['uri']
            logger.info(f"Post published successfully: {post_uri}")
            return True
        else:
            logger.error("The response object does not contain 'uri' attribute.")
            return False
    except Exception as e:
        logger.error(f"Failed to post to Bluesky: {e}")
        return False


@click.command()
@click.option(
    "-n",
    "--new-repos-file",
    default="new_repos.csv",
    help="Path to CSV file containing new repos",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Don't actually post, just show what would be posted",
)
def cli(new_repos_file: str, dry_run: bool) -> None:
    """Post new repositories to Bluesky."""
    new_repos_path = THIS_DIR / new_repos_file

    # Check if new repos file exists
    if not new_repos_path.exists():
        print(f"[yellow]No new repos file found at {new_repos_path}[/yellow]")
        print("[yellow]Nothing to post.[/yellow]")
        sys.exit(0)

    # Load new repos
    try:
        new_repos_df = pd.read_csv(new_repos_path)
    except Exception as e:
        print(f"[red]Error reading new repos file: {e}[/red]")
        sys.exit(1)

    if len(new_repos_df) == 0:
        print("[yellow]No new repositories to post.[/yellow]")
        sys.exit(0)

    print(f"[cyan]Found {len(new_repos_df)} new repo(s) to post[/cyan]")

    # Get credentials from environment variables
    username = os.getenv("BLUESKY_USERNAME")
    password = os.getenv("BLUESKY_PASSWORD")

    if not username or not password:
        print("[red]Error: BLUESKY_USERNAME and BLUESKY_PASSWORD environment variables must be set[/red]")
        sys.exit(1)

    if dry_run:
        print("[yellow]DRY RUN MODE - Not actually posting[/yellow]")
        for _, repo in new_repos_df.iterrows():
            record = create_repo_post(repo.to_dict())
            print(f"\n[cyan]Would post:[/cyan]")
            print(f"  {record['text']}")
        sys.exit(0)

    # Authenticate to Bluesky
    client = authenticate_bluesky(username, password)

    # Post each new repo
    success_count = 0
    for _, repo in new_repos_df.iterrows():
        print(f"\n[cyan]Posting: {repo['full_name']}[/cyan]")
        record = create_repo_post(repo.to_dict())

        if post_to_bluesky(client, record):
            success_count += 1
            print(f"[green]✓ Successfully posted {repo['full_name']}[/green]")
        else:
            print(f"[red]✗ Failed to post {repo['full_name']}[/red]")

    print(f"\n[green]Posted {success_count}/{len(new_repos_df)} repositories[/green]")

    if success_count == len(new_repos_df):
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    cli()
