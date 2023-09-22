import argparse
import json
from datetime import datetime
from typing import List, Tuple

from listenbrainz import ListenBrainzClient, Track


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def clean_additional_info(info):
    for (k, v) in dict(info).items():
        if not v:
            del info[k]
    return info


def process_spotify_import(file) -> List[Tuple[int, Track]]:
    listens = []
    with open(file, mode="r", encoding="utf-8") as f:
        streams = json.load(f)

        for stream in streams:
            if stream["ms_played"] < 30000:
                # played for less than 30s so skip
                continue

            ts = stream["ts"][:-1]
            listened_at = int(datetime.fromisoformat(ts).timestamp())
            track = Track.from_dict({
                "track_name": stream["master_metadata_track_name"],
                "artist_name": stream["master_metadata_album_artist_name"],
                "release_name": stream["master_metadata_album_album_name"],
                "additional_info": {
                    "duration_ms": stream["ms_played"],
                    "music_service": "spotify.com",
                    "submission_client": "ListenBrainz Import Script (outsidecontext / lucifer)",
                    "spotify_track_uri": stream["spotify_track_uri"]
                }
            })

            listens.append((listened_at, track))

    return listens


def submit_listens(token, file):
    listens = process_spotify_import(file)

    total_count = len(listens)
    submitted_count = 0
    print("Read %i listens" % total_count)

    client = ListenBrainzClient()
    client.user_token = token

    listens = sorted(listens, key=lambda k: k[0], reverse=True)

    for submission in chunks(listens, 200):
        client.import_tracks(submission)
        submitted_count += len(submission)
        print("Submitted %i/%i listens" % (submitted_count, total_count))

    print("Submitted all listens")


parser = argparse.ArgumentParser()
parser.add_argument("token", help="ListenBrainz authentication token", type=str)
parser.add_argument("file", help="path to the scrobble.log file", type=str)
args = parser.parse_args()

submit_listens(args.token, args.file)
