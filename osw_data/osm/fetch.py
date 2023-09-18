from pathlib import Path

import click
import requests

CHUNK_SIZE = 8192


def osm_fetch(url, workdir, progressbar=False):
    workdir_path = Path(workdir)
    filename = Path(url).name
    filepath = Path(workdir_path, filename)

    if not workdir_path.exists():
        workdir_path.mkdir()

    # FIXME: provide progress feedback
    with requests.get(url, stream=True) as r:
        r.raise_for_status()

        if progressbar:
            size = int(r.headers["Content-Length"].strip())
            pbar = click.progressbar(
                length=size, label=f"    downloading {filename}"
            )

        with open(filepath, "wb") as f:
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                f.write(chunk)
                if progressbar:
                    pbar.update(len(chunk))

        if progressbar:
            pbar.render_finish()

    return filepath
