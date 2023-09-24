"""
Simple importer for ViMusic exports
"""

from datetime import datetime
from pathlib import Path
from typing import Generator
import sqlite3
from sqlite3 import Connection
import argparse

from listenbrainz import ListenBrainzClient, Track


def chunks(input_list: list, chunk_size: int) -> Generator:
    """
    Yield successive chunk_size-sized chunks from input_list.
    """
    for index in range(0, len(input_list), chunk_size):
        yield input_list[index : index + chunk_size]


def connect_db_file(file: Path) -> Connection:
    """
    Connect
    """
    return sqlite3.connect(file)


def process_track_duration(duration: str) -> int:
    """
    Return the track duration in seconds from the given string
    Assumes the string is in the H:M:S format with H being conditional
    """
    second_duration = 0
    multiplier = (1, 60, 3600)
    if not duration:
        return second_duration
    try:
        for index, value in enumerate(duration.split(":")[::-1]):
            second_duration += int(value) * multiplier[index]
    except ValueError:
        print("Issue while parsing duration, skipping this data")
        second_duration = 0
    return second_duration


def parse_youtube_id(song_id: str) -> str:
    """
    Return the YouTube URL for a given song
    """
    return f"https://www.youtube.com/watch?v={song_id}"


def process_vimusic_import(file: Path) -> list[tuple[int, Track]]:
    """
    Extract music info from the db backup and return it as a list of listens
    """
    listens = []
    data = connect_db_file(file)
    for song_id, timestamp in data.execute("SELECT songId,timestamp FROM Event"):
        # timestamps are recorded in nanoseconds in vimusic backups currently
        listened_at = int(datetime.fromtimestamp(timestamp // 1000).timestamp())
        title, artist, duration = data.execute(
            f"SELECT title,artistsText,durationText FROM Song WHERE id='{song_id}'"
        ).fetchone()

        # mandatory elements for a song must be met
        if not artist or not title:
            continue

        track = Track.from_dict(
            {
                "track_name": title,
                "artist_name": artist,
                "additional_info": {
                    "duration": process_track_duration(duration),
                    "music_service": "youtube.com",
                    "submission_client": "ListenBrainz Import Script (Static_Rocket)",
                    "origin_url": parse_youtube_id(song_id),
                },
            }
        )
        listens.append((listened_at, track))
    return listens


def submit_listens(token: str, file: Path) -> None:
    """
    Submit listens from the given file to ListenBrainz using the token
    """
    listens = process_vimusic_import(file)

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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("token", help="ListenBrainz authentication token", type=str)
    parser.add_argument("file", help="path to the export file", type=Path)
    args = parser.parse_args()

    submit_listens(args.token, args.file)
