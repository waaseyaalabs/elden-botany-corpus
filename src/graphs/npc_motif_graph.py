"""NPC motif graph scaffolding for Phase 7 analysis."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import click

GraphType = Any


@dataclass(slots=True)
class NpcMotifGraphConfig:
    """Configuration options for building the NPC → motif graph."""

    curated_path: Path | None = None
    coverage_path: Path | None = None
    community_dir: Path | None = None
    output_dir: Path = Path("data/analysis")
    min_edge_weight: int = 1
    include_dialogue_edges: bool = True
    include_annotation_edges: bool = True


@dataclass(slots=True)
class NpcMotifGraphArtifacts:
    """Artifact paths emitted by the graph pipeline."""

    graphml_path: Path
    json_path: Path
    metrics_path: Path | None = None


class NpcMotifGraphBuilder:
    """Responsible for orchestrating graph construction and export."""

    def __init__(self, config: NpcMotifGraphConfig | None = None) -> None:
        self.config = config or NpcMotifGraphConfig()

    def build(self) -> GraphType:
        """Construct the NPC → motif graph.

        TODO(phase-7): Implement lore loading, motif linking, and NetworkX
        graph creation once upstream data contracts are finalized.
        """

        raise NotImplementedError(
            "NPC motif graph builder not implemented yet."
        )

    def export(self, graph: GraphType) -> NpcMotifGraphArtifacts:
        """Write graph artifacts to disk.

        TODO(phase-7): Persist GraphML + JSON exports and summary metrics.
        """

        raise NotImplementedError("Graph export is not available yet.")

    def npc_report(self, graph: GraphType, npc_slug: str) -> dict[str, Any]:
        """Produce motif ranking report for a single NPC."""

        raise NotImplementedError

    def motif_report(
        self, graph: GraphType, motif_slug: str
    ) -> dict[str, Any]:
        """Produce NPC ranking report for a single motif."""

        raise NotImplementedError


@click.command()
@click.option(
    "--npc",
    type=str,
    default=None,
    help="Optional NPC slug to focus the report on.",
)
@click.option(
    "--motif",
    type=str,
    default=None,
    help="Optional motif slug to filter the report.",
)
@click.option(
    "--export/--no-export",
    default=False,
    help="Toggle export of graph artifacts to data/analysis.",
)
@click.option(
    "--min-edge-weight",
    type=int,
    default=None,
    help="Override minimum edge weight for inclusion.",
)
def main(
    npc: str | None,
    motif: str | None,
    export: bool,
    min_edge_weight: int | None,
) -> None:
    """Placeholder CLI entrypoint for NPC motif graph analysis."""

    config = NpcMotifGraphConfig(
        min_edge_weight=min_edge_weight
        or NpcMotifGraphConfig().min_edge_weight,
    )
    builder = NpcMotifGraphBuilder(config)
    raise click.ClickException(
        "NPC motif graph analysis is not available yet. "
        f"Args npc={npc}, motif={motif}, export={export}, "
        f"min_edge={builder.config.min_edge_weight}"
    )


if __name__ == "__main__":  # pragma: no cover
    main()
