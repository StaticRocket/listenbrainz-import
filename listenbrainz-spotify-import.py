"""
Tool to import Spotify playlist exports from watsonbox/exportify as listens
"""

import argparse
from datetime import datetime
from typing import List, Tuple, Generator
from pathlib import Path
import pandas
import requests

from listenbrainz import ListenBrainzClient, Track


def chunks(input_list: list, chunk_size: int) -> Generator:
    """
    Yield successive chunk_size-sized chunks from input_list.
    """
    for index in range(0, len(input_list), chunk_size):
        yield input_list[index : index + chunk_size]


def process_spotify_import(file) -> List[Tuple[int, Track]]:
    """
    Process given csv file and unravel it as a list of listens
    """
    listens = []
    export_df = pandas.read_csv(file)

    for _, stream in export_df.iterrows():
        listened_at = int(datetime.fromisoformat(stream["Added At"]).timestamp())
        metadata = lookup_metadata(stream["Track Name"], stream["Album Artist Name(s)"], None)
        if not metadata:
            continue
        track = Track.from_dict(
            {
                "track_name": stream["Track Name"],
                "artist_name": stream["Album Artist Name(s)"],
                "release_name": stream["Album Name"],
                "additional_info": {
                    "duration_ms": stream["Track Duration (ms)"],
                    "music_service": "spotify.com",
                    "submission_client": "ListenBrainz Import Script (Static_Rocket)",
                    "origin_url": parse_spotify_uri(stream["Track URI"]),
                },
                "recording_mbid": metadata["recording_mbid"],
            }
        )

        listens.append((listened_at, track))

    return listens


def parse_spotify_uri(uri: str) -> str:
    """
    Parse URI and hand back a
    """
    track_id = uri.split(":")[-1]
    return f"https://open.spotify.com/track/{track_id}"


def submit_listens(token: str, file: Path) -> None:
    """
    Submit listens from the given file to ListenBrainz using the token
    """
    listens = process_spotify_import(file)

    total_count = len(listens)
    submitted_count = 0
    print(f"Read {total_count} listens")

    client = ListenBrainzClient()
    client.user_token = token  # type: ignore

    listens = sorted(listens, key=lambda k: k[0], reverse=True)

    for submission in chunks(listens, 200):
        client.import_tracks(submission)
        submitted_count += len(submission)
        print(f"Submitted {submitted_count}/{total_count} listens")

    print("Submitted all listens")

def submit_feedback(token: str, file: Path) -> None:
    """
    Submit listens from the given file to ListenBrainz using the token
    """
    listens = process_spotify_import(file)

    total_count = len(listens)
    submitted_count = 0
    print(f"Read {total_count} listens")

    client = ListenBrainzClient()
    client.user_token = token  # type: ignore

    listens = sorted(listens, key=lambda k: k[0], reverse=True)

    for _, submission in listens:
        client.submit_feedback(submission, 1)
        submitted_count += 1
        print(f"Submitted {submitted_count}/{total_count} listens")

    print("Submitted all listens")



def lookup_metadata(track_name: str, artist_name: str, incs: str) -> dict:
    """Looks up the metadata for a listen using track name and artist name."""
    params = {
        "recording_name": track_name,
        "artist_name": artist_name
    }
    if incs:
        params["metadata"] = True
        params["incs"] = incs
    response = requests.get(
        url="https://api.listenbrainz.org/1/metadata/lookup/",
        params=params
    )
    response.raise_for_status()
    print(response.json())
    return response.json()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("token", help="ListenBrainz authentication token", type=str)
    parser.add_argument("file", help="path to the scrobble.log file", type=Path)
    args = parser.parse_args()

    submit_feedback(args.token, args.file)
