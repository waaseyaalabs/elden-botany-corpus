"""Downloader for Carian Archive FMG localization files."""

from __future__ import annotations

from pathlib import Path
from typing import Final

import requests

from corpus.config import settings


def _relpath(filename: str) -> str:
    return f"GameText/GR/data/INTERROOT_win64/msg/engUS/{filename}"


CARIAN_ARCHIVE_REPO: Final = "AsteriskAmpersand/Carian-Archive"
CARIAN_BASE_URL: Final = (
    f"https://raw.githubusercontent.com/{CARIAN_ARCHIVE_REPO}/main"
)
CARIAN_FMG_FILENAMES: Final[tuple[str, ...]] = (
    "AccessoryCaption.fmg.xml",
    "AccessoryInfo.fmg.xml",
    "AccessoryName.fmg.xml",
    "ActionButtonText.fmg.xml",
    "ArtsCaption.fmg.xml",
    "ArtsName.fmg.xml",
    "BloodMsg.fmg.xml",
    "BossCaption.fmg.xml",
    "BossName.fmg.xml",
    "EventTextForMap.fmg.xml",
    "EventTextForTalk.fmg.xml",
    "GemCaption.fmg.xml",
    "GemEffect.fmg.xml",
    "GemInfo.fmg.xml",
    "GemName.fmg.xml",
    "GoodsCaption.fmg.xml",
    "GoodsDialog.fmg.xml",
    "GoodsInfo.fmg.xml",
    "GoodsInfo2.fmg.xml",
    "GoodsName.fmg.xml",
    "GR_Dialogues.fmg.xml",
    "GR_KeyGuide.fmg.xml",
    "GR_LineHelp.fmg.xml",
    "GR_MenuText.fmg.xml",
    "GR_System_Message_win64.fmg.xml",
    "LoadingText.fmg.xml",
    "LoadingTitle.fmg.xml",
    "MagicCaption.fmg.xml",
    "MagicInfo.fmg.xml",
    "MagicName.fmg.xml",
    "MovieSubtitle.fmg.xml",
    "NetworkMessage.fmg.xml",
    "NpcName.fmg.xml",
    "PlaceName.fmg.xml",
    "ProtectorCaption.fmg.xml",
    "ProtectorInfo.fmg.xml",
    "ProtectorName.fmg.xml",
    "TalkMsg.fmg.xml",
    "TextEmbedImageName_win64.fmg.xml",
    "ToS_win64.fmg.xml",
    "TutorialBody.fmg.xml",
    "TutorialTitle.fmg.xml",
    "WeaponCaption.fmg.xml",
    "WeaponEffect.fmg.xml",
    "WeaponInfo.fmg.xml",
    "WeaponName.fmg.xml",
    "WeaponSkillCaption.fmg.xml",
    "WeaponSkillName.fmg.xml",
)

CARIAN_FMG_RELATIVE_PATHS: Final[list[str]] = [
    _relpath(filename) for filename in CARIAN_FMG_FILENAMES
]
REQUEST_TIMEOUT_SECONDS: Final = 30


class CarianFMGDownloader:
    """Download the subset of Carian Archive FMG XML files we consume."""

    def __init__(self, base_dir: Path | None = None) -> None:
        self.base_dir = base_dir or (settings.raw_dir / "carian_archive")
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def fetch(self, *, force: bool = False) -> list[Path]:
        """Download every required FMG file, returning their paths."""

        downloaded: list[Path] = []
        for relative_path in CARIAN_FMG_RELATIVE_PATHS:
            downloaded.append(self._download_file(relative_path, force=force))
        return downloaded

    def _download_file(self, relative_path: str, *, force: bool) -> Path:
        target_path = self.base_dir / relative_path
        url = f"{CARIAN_BASE_URL}/{relative_path}"

        if target_path.exists() and not force:
            print(f"Using cached FMG file: {target_path}")
            return target_path

        print(f"Downloading {relative_path} from Carian Archive…")
        response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()

        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(response.content)
        print(f"Saved {target_path} ({len(response.content)} bytes)")
        return target_path


def fetch_carian_fmg_files(*, force: bool = False) -> list[Path]:
    """Public convenience wrapper for downloading FMG XML assets."""

    print("\n=== Fetching Carian Archive FMG XMLs ===")
    downloader = CarianFMGDownloader()
    return downloader.fetch(force=force)
