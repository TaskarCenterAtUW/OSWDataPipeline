import asyncio
import os
from pathlib import Path
import subprocess
import tempfile


class OSMClipError(Exception):
    pass


# FIXME: instead of dict, use dataclass for config region schema
async def osm_clip(
    in_pbf_path: str, out_pbf_path: str, polygon_feature: dict, mem="256m"
):
    """Clips an OSM"""

    env = {"JAVA_OPTS": "-Xmx256m", **os.environ}

    with tempfile.TemporaryDirectory() as tmpdirname:
        temporary_path = Path(tmpdirname, "region.poly")
        with open(temporary_path, "w") as fp:
            lines = []
            lines.append(f"{polygon_feature['properties']['id']}")
            for i, inner_poly in enumerate(
                polygon_feature["geometry"]["coordinates"][0]
            ):
                # Write a name for the polygon
                lines.append(f"area_{i}")
                for lon, lat in inner_poly:
                    lines.append(f"\t{lon}\t{lat}")
                lines.append("END")
            lines.append("END")
            lines = [f"{line}\n" for line in lines]
            fp.writelines(lines)

        try:
            # FIXME: when input polygon is bad, this doesn't raise a
            # subprocess.CalledProcessError. How else can we catch this error?
            process = await asyncio.create_subprocess_exec(
                "osmosis",
                "--read-pbf",
                f"file={in_pbf_path}",
                "--bounding-polygon",
                "completeWays=yes",
                f"file={temporary_path}",
                "--write-pbf",
                f"file={out_pbf_path}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
            stdout, stderr = await process.communicate()
        except subprocess.CalledProcessError as e:
            raise OSMClipError(e.message)
