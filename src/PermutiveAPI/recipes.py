"""Discoverable executable recipes for the canonical PermutiveAPI surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable


class RecipeCategory(str, Enum):
    """Supported recipe discovery categories."""

    SDK = "sdk"
    ASYNC = "async"
    QUERIES = "queries"
    PLUGIN = "plugin"
    GOVERNED = "governed"


@dataclass(frozen=True)
class Recipe:
    """Describe one copy-paste executable recipe."""

    name: str
    category: RecipeCategory
    description: str
    source: str
    credential_free: bool = True

    def to_dict(self) -> dict[str, object]:
        """Return deterministic machine-readable recipe metadata."""
        return {
            "name": self.name,
            "category": self.category.value,
            "description": self.description,
            "source": self.source,
            "credential_free": self.credential_free,
        }


_RECIPES = (
    Recipe(
        name="workspace-inspection",
        category=RecipeCategory.SDK,
        description="Inspect a workspace through the canonical synchronous client.",
        source="""from PermutiveAPI import PermutiveClient, RetryPolicy
from PermutiveAPI.testing import MockPermutiveServer, MockResponse, MockRoute


def main() -> dict[str, object]:
    routes = (
        MockRoute(
            "GET",
            "/v1/workspaces/current",
            (MockResponse(body={"id": "workspace-demo", "name": "Demo"}),),
        ),
    )
    with MockPermutiveServer(routes) as server:
        with PermutiveClient(
            "local-demo-key",
            base_url=server.base_url,
            retry_policy=RetryPolicy(max_attempts=1),
        ) as client:
            return client.request("GET", "v1/workspaces/current")


if __name__ == "__main__":
    print(main())
""",
    ),
    Recipe(
        name="cohort-analysis",
        category=RecipeCategory.SDK,
        description="List cohorts and calculate a deterministic local summary.",
        source="""from PermutiveAPI import PermutiveClient, RetryPolicy
from PermutiveAPI.testing import MockPermutiveServer, MockResponse, MockRoute


def main() -> dict[str, object]:
    routes = (
        MockRoute(
            "GET",
            "/v1/cohorts",
            (MockResponse(body={"items": [{"id": "a"}, {"id": "b"}]}),),
        ),
    )
    with MockPermutiveServer(routes) as server:
        with PermutiveClient(
            "local-demo-key",
            base_url=server.base_url,
            retry_policy=RetryPolicy(max_attempts=1),
        ) as client:
            payload = client.request("GET", "v1/cohorts")
    items = payload.get("items", [])
    return {"cohort_count": len(items) if isinstance(items, list) else 0}


if __name__ == "__main__":
    print(main())
""",
    ),
    Recipe(
        name="segment-comparison",
        category=RecipeCategory.SDK,
        description="Compare two segment sizes through canonical client requests.",
        source="""from PermutiveAPI import PermutiveClient, RetryPolicy
from PermutiveAPI.testing import MockPermutiveServer, MockResponse, MockRoute


def main() -> dict[str, int]:
    routes = (
        MockRoute("GET", "/v1/segments/left", (MockResponse(body={"size": 90}),)),
        MockRoute("GET", "/v1/segments/right", (MockResponse(body={"size": 35}),)),
    )
    with MockPermutiveServer(routes) as server:
        with PermutiveClient(
            "local-demo-key",
            base_url=server.base_url,
            retry_policy=RetryPolicy(max_attempts=1),
        ) as client:
            left = client.request("GET", "v1/segments/left")
            right = client.request("GET", "v1/segments/right")
    return {"difference": int(left["size"]) - int(right["size"])}


if __name__ == "__main__":
    print(main())
""",
    ),
    Recipe(
        name="async-workspace-inspection",
        category=RecipeCategory.ASYNC,
        description="Inspect a workspace with the optional HTTPX async client.",
        source="""import asyncio

from PermutiveAPI import AsyncPermutiveClient, RetryPolicy
from PermutiveAPI.testing import MockPermutiveServer, MockResponse, MockRoute


async def run() -> dict[str, object]:
    routes = (
        MockRoute(
            "GET",
            "/v1/workspaces/current",
            (MockResponse(body={"id": "async-demo", "name": "Async Demo"}),),
        ),
    )
    with MockPermutiveServer(routes) as server:
        async with AsyncPermutiveClient(
            "local-demo-key",
            base_url=server.base_url,
            retry_policy=RetryPolicy(max_attempts=1),
        ) as client:
            return await client.request("GET", "v1/workspaces/current")


def main() -> dict[str, object]:
    return asyncio.run(run())


if __name__ == "__main__":
    print(main())
""",
    ),
    Recipe(
        name="typed-query",
        category=RecipeCategory.QUERIES,
        description="Compose a deterministic typed event and segment query.",
        source="""from PermutiveAPI import all_of, event, in_segment


def main() -> dict[str, object]:
    return all_of((event("pageview"), in_segment("high-intent"))).to_json()


if __name__ == "__main__":
    print(main())
""",
    ),
    Recipe(
        name="codex-plugin-discovery",
        category=RecipeCategory.PLUGIN,
        description="Discover the Codex plugin contract without a network request.",
        source="""from PermutiveAPI.credentials import LocalCredentialsProvider
from PermutiveAPI.plugins.codex import CodexPlugin


def main() -> dict[str, object]:
    plugin = CodexPlugin(LocalCredentialsProvider(api_key="local-demo-key"))
    try:
        return {
            "name": plugin.metadata.name,
            "api_version": plugin.metadata.api_version,
            "tool_count": plugin.tools().capabilities()["tool_count"],
        }
    finally:
        plugin.close()


if __name__ == "__main__":
    print(main())
""",
    ),
    Recipe(
        name="reviewed-cohort-write",
        category=RecipeCategory.GOVERNED,
        description="Execute an explicitly approved reviewed write with audit evidence.",
        source="""from PermutiveAPI import PermutiveClient, RetryPolicy
from PermutiveAPI.scenario_fixtures import scenario_mock_routes
from PermutiveAPI.scenarios import GovernedScenarioRunner, ScenarioRequest
from PermutiveAPI.testing import MockPermutiveServer


def main() -> dict[str, object]:
    with MockPermutiveServer(scenario_mock_routes()) as server:
        with PermutiveClient(
            "local-demo-key",
            base_url=server.base_url,
            retry_policy=RetryPolicy(max_attempts=1),
        ) as client:
            result = GovernedScenarioRunner(client).run(
                ScenarioRequest(
                    "Create a reviewed cohort named example",
                    "recipe-reviewed-write",
                    approved=True,
                )
            )
    return result.to_dict()


if __name__ == "__main__":
    print(main())
""",
    ),
)


def recipe_catalog() -> tuple[Recipe, ...]:
    """Return every canonical recipe in deterministic order."""
    return tuple(sorted(_RECIPES, key=lambda item: (item.category.value, item.name)))


def find_recipes(
    *,
    category: RecipeCategory | str | None = None,
    name: str | None = None,
) -> tuple[Recipe, ...]:
    """Filter recipes by exact category and optional exact name."""
    resolved = RecipeCategory(category) if isinstance(category, str) else category
    recipes: Iterable[Recipe] = recipe_catalog()
    if resolved is not None:
        recipes = (item for item in recipes if item.category is resolved)
    if name is not None:
        recipes = (item for item in recipes if item.name == name)
    return tuple(recipes)


def recipe_manifest() -> dict[str, object]:
    """Return the versioned machine-readable recipe catalog."""
    return {
        "version": 1,
        "categories": [category.value for category in RecipeCategory],
        "recipes": [recipe.to_dict() for recipe in recipe_catalog()],
    }


__all__ = [
    "Recipe",
    "RecipeCategory",
    "find_recipes",
    "recipe_catalog",
    "recipe_manifest",
]
