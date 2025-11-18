"""Downloader for Carian Archive FMG localization files."""

from __future__ import annotations

from pathlib import Path
from typing import Final

import requests

from corpus.config import settings


def _relpath(filename: str) -> str:
    return f"GameText/GR/data/INTERROOT_win64/msg/engUS/{filename}"


CARIAN_ARCHIVE_REPO: Final = "AsteriskAmpersand/Carian-Archive"
CARIAN_BASE_URL: Final = f"https://raw.githubusercontent.com/{CARIAN_ARCHIVE_REPO}/main"
CARIAN_FMG_CANDIDATES: Final[dict[str, tuple[str, ...]]] = {
    "accessory_caption": ("AccessoryCaption.fmg.xml",),
    "accessory_info": ("AccessoryInfo.fmg.xml",),
    "accessory_name": ("AccessoryName.fmg.xml",),
    "action_button_text": ("ActionButtonText.fmg.xml",),
    "blood_msg": ("BloodMsg.fmg.xml",),
    "boss_caption": ("BossCaption.fmg.xml",),
    "boss_name": ("BossName.fmg.xml",),
    "event_text_map": ("EventTextForMap.fmg.xml",),
    "event_text_talk": ("EventTextForTalk.fmg.xml",),
    "gem_caption": ("GemCaption.fmg.xml",),
    "gem_effect": ("GemEffect.fmg.xml",),
    "gem_info": ("GemInfo.fmg.xml",),
    "gem_name": ("GemName.fmg.xml",),
    "goods_caption": ("GoodsCaption.fmg.xml",),
    "goods_dialog": ("GoodsDialog.fmg.xml",),
    "goods_info": ("GoodsInfo.fmg.xml",),
    "goods_info_2": ("GoodsInfo2.fmg.xml",),
    "goods_name": ("GoodsName.fmg.xml",),
    "gr_dialogues": ("GR_Dialogues.fmg.xml",),
    "gr_key_guide": ("GR_KeyGuide.fmg.xml",),
    "gr_line_help": ("GR_LineHelp.fmg.xml",),
    "gr_menu_text": ("GR_MenuText.fmg.xml",),
    "gr_system_message": ("GR_System_Message_win64.fmg.xml",),
    "loading_text": ("LoadingText.fmg.xml",),
    "loading_title": ("LoadingTitle.fmg.xml",),
    "magic_caption": ("MagicCaption.fmg.xml",),
    "magic_info": ("MagicInfo.fmg.xml",),
    "magic_name": ("MagicName.fmg.xml",),
    "movie_subtitle": ("MovieSubtitle.fmg.xml",),
    "network_message": ("NetworkMessage.fmg.xml",),
    "npc_name": ("NpcName.fmg.xml",),
    "place_name": ("PlaceName.fmg.xml",),
    "protector_caption": ("ProtectorCaption.fmg.xml",),
    "protector_info": ("ProtectorInfo.fmg.xml",),
    "protector_name": ("ProtectorName.fmg.xml",),
    "talk": ("TalkMsg.fmg.xml",),
    "text_embed_image_name": ("TextEmbedImageName_win64.fmg.xml",),
    "tos": ("ToS_win64.fmg.xml",),
    "tutorial_body": ("TutorialBody.fmg.xml",),
    "tutorial_title": ("TutorialTitle.fmg.xml",),
    "weapon_caption": ("WeaponCaption.fmg.xml",),
    "weapon_effect": ("WeaponEffect.fmg.xml",),
    "weapon_info": ("WeaponInfo.fmg.xml",),
    "weapon_name": ("WeaponName.fmg.xml",),
    "weapon_skill": (
        "WeaponSkillName.fmg.xml",
        "ArtsName.fmg.xml",
    ),
    "weapon_skill_caption": (
        "WeaponSkillCaption.fmg.xml",
        "ArtsCaption.fmg.xml",
    ),
}

CARIAN_FMG_RELATIVE_PATHS: Final[list[str]] = [
    _relpath(candidates[0]) for candidates in CARIAN_FMG_CANDIDATES.values()
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
        for dataset, candidates in CARIAN_FMG_CANDIDATES.items():
            path = self._download_candidates(dataset, candidates, force=force)
            if path is not None:
                downloaded.append(path)
        return downloaded

    def _download_candidates(
        self,
        dataset: str,
        candidates: tuple[str, ...],
        *,
        force: bool,
    ) -> Path | None:
        if not force:
            for filename in candidates:
                relative_path = _relpath(filename)
                target_path = self.base_dir / relative_path
                if target_path.exists():
                    print(f"[{dataset}] Using cached FMG file: {target_path}")
                    return target_path

        last_error: str | None = None
        for filename in candidates:
            relative_path = _relpath(filename)
            target_path = self.base_dir / relative_path
            url = f"{CARIAN_BASE_URL}/{relative_path}"
            print(f"[{dataset}] Downloading candidate {filename} " "from Carian Archive…")
            response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
            if response.status_code == 404:
                last_error = relative_path
                print(f"[{dataset}] Candidate missing from Carian Archive: " f"{filename}")
                continue
            response.raise_for_status()

            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_bytes(response.content)
            print(f"[{dataset}] Saved {target_path} " f"({len(response.content)} bytes)")
            return target_path

        if last_error is not None:
            print(
                f"[{dataset}] Unable to download any FMG candidates; "
                f"tried {', '.join(candidates)}"
            )
        return None


def fetch_carian_fmg_files(*, force: bool = False) -> list[Path]:
    """Public convenience wrapper for downloading FMG XML assets."""

    print("\n=== Fetching Carian Archive FMG XMLs ===")
    downloader = CarianFMGDownloader()
    return downloader.fetch(force=force)
