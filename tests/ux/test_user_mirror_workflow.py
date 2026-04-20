"""UX test modelling the structure of mirror/workflows without real I/O.

The mirror workflow is the primary use-case for zahir: a sequential top-level
orchestrator that fans out to parallel child jobs, handles scan failures
gracefully, and conditionally dispatches a final step.
"""

from tertius import EEmit

from zahir.core.effects import EAwaitAll
from zahir.core.evaluate import JobContext, evaluate


# --- mock leaf jobs (no-ops that mirror real job signatures) ---


def media_scan(ctx: JobContext, input: dict):
    return {"complete": True}
    yield


def read_albums(ctx: JobContext, input: dict):
    return {"status": "albums_loaded"}
    yield


def read_photos(ctx: JobContext, input: dict):
    return {"status": "photos_loaded"}
    yield


def read_videos(ctx: JobContext, input: dict):
    return {"status": "videos_loaded"}
    yield


def wikidata_scan(ctx: JobContext, input: dict):
    return {"complete": True}
    yield


def compute_contrasting_grey(ctx: JobContext, input: dict):
    return None
    yield


def compute_image_mosaic(ctx: JobContext, input: dict):
    return None
    yield


def upload_missing_photos(ctx: JobContext, input: dict):
    return None
    yield


def upload_missing_videos(ctx: JobContext, input: dict):
    return None
    yield


def publish_artifacts(ctx: JobContext, input: dict):
    yield EEmit({"published": True})


def build_website(ctx: JobContext, input: dict):
    yield EEmit({"website_built": True})


# --- mock orchestrators (structural mirror of mirror/workflows) ---


def scan_media(ctx: JobContext, input: dict):
    """Mirrors scan.scan_media: sequential scan then parallel reads then wikidata."""
    yield ctx.scope.media_scan({})

    yield EAwaitAll(
        [
            ctx.scope.read_albums({"markdown_path": input.get("albums_markdown_path")}),
            ctx.scope.read_photos({"markdown_path": input.get("photos_markdown_path")}),
            ctx.scope.read_videos({}),
        ]
    )

    yield ctx.scope.wikidata_scan({})


def failing_scan_media(ctx: JobContext, input: dict):
    """Variant that raises, to test mirror_workflow's scan error handling."""
    raise RuntimeError("vault unavailable")
    yield


def upload_media(ctx: JobContext, input: dict):
    """Mirrors upload.upload_media: parallel grey/mosaic, then optional uploads."""
    grey_effects = [
        ctx.scope.compute_contrasting_grey({"fpath": f})
        for f in input.get("fpaths", [])
    ]
    if grey_effects:
        yield EAwaitAll(grey_effects)

    mosaic_effects = [
        ctx.scope.compute_image_mosaic({"fpath": f}) for f in input.get("fpaths", [])
    ]
    if mosaic_effects:
        yield EAwaitAll(mosaic_effects)

    if input.get("upload_images"):
        photo_effects = [
            ctx.scope.upload_missing_photos({"fpath": f})
            for f in input.get("fpaths", [])
        ]
        if photo_effects:
            yield EAwaitAll(photo_effects)

    if input.get("upload_videos"):
        for fpath in input.get("fpaths", []):
            yield ctx.scope.upload_missing_videos({"fpath": fpath})


def mirror_workflow(ctx: JobContext, input: dict):
    """Mirrors workflow.mirror_workflow: the top-level orchestrator."""
    try:
        yield ctx.scope.scan_media(
            {
                "albums_markdown_path": input.get("albums_markdown_path"),
                "photos_markdown_path": input.get("photos_markdown_path"),
            }
        )
    except Exception as err:
        print(f"WARNING: scan_media failed, continuing to publish: {err}")

    yield ctx.scope.upload_media(
        {
            "force_recompute_grey": input.get("force_recompute_grey", False),
            "force_recompute_mosaic": input.get("force_recompute_mosaic", False),
            "upload_images": input.get("upload_images"),
            "upload_videos": input.get("upload_videos"),
            "fpaths": input.get("fpaths", []),
        }
    )

    yield ctx.scope.publish_artifacts({})

    if input.get("publish_d1"):
        yield ctx.scope.build_website({})


# --- shared scope (all jobs required for pickling) ---

BASE_SCOPE = {
    "mirror_workflow": mirror_workflow,
    "scan_media": scan_media,
    "upload_media": upload_media,
    "media_scan": media_scan,
    "read_albums": read_albums,
    "read_photos": read_photos,
    "read_videos": read_videos,
    "wikidata_scan": wikidata_scan,
    "compute_contrasting_grey": compute_contrasting_grey,
    "compute_image_mosaic": compute_image_mosaic,
    "upload_missing_photos": upload_missing_photos,
    "upload_missing_videos": upload_missing_videos,
    "publish_artifacts": publish_artifacts,
    "build_website": build_website,
}


# --- tests ---


def test_mirror_workflow_runs_and_publishes():
    """Proves a mirror-shaped workflow completes and emits a publish event."""

    events = list(
        evaluate("mirror_workflow", ({"publish_d1": False},), BASE_SCOPE, n_workers=4)
    )

    assert events == [{"published": True}]


def test_mirror_workflow_builds_website_when_publish_d1():
    """Proves build_website is dispatched only when publish_d1 is set."""

    events = list(
        evaluate("mirror_workflow", ({"publish_d1": True},), BASE_SCOPE, n_workers=4)
    )

    assert events == [{"published": True}, {"website_built": True}]


def test_mirror_workflow_continues_after_scan_failure():
    """Proves mirror_workflow swallows scan errors and still publishes."""

    scope = {**BASE_SCOPE, "scan_media": failing_scan_media}
    events = list(
        evaluate("mirror_workflow", ({"publish_d1": False},), scope, n_workers=4)
    )

    assert events == [{"published": True}]


def emitting_media_scan(ctx: JobContext, input: dict):
    yield EEmit("media_scan")


def emitting_wikidata_scan(ctx: JobContext, input: dict):
    yield EEmit("wikidata_scan")


def emitting_upload_missing_photos(ctx: JobContext, input: dict):
    yield EEmit("upload_missing_photos")


def emitting_upload_missing_videos(ctx: JobContext, input: dict):
    yield EEmit("upload_missing_videos")


def emitting_publish_artifacts(ctx: JobContext, input: dict):
    yield EEmit("publish_artifacts")


def emitting_build_website(ctx: JobContext, input: dict):
    yield EEmit("build_website")


def emitting_read_albums(ctx: JobContext, input: dict):
    yield EEmit("read_albums")


def emitting_read_photos(ctx: JobContext, input: dict):
    yield EEmit("read_photos")


def emitting_read_videos(ctx: JobContext, input: dict):
    yield EEmit("read_videos")


def emitting_grey(ctx: JobContext, input: dict):
    yield EEmit(("grey", input["fpath"]))


def emitting_mosaic(ctx: JobContext, input: dict):
    yield EEmit(("mosaic", input["fpath"]))


def test_mirror_workflow_parallel_scan_reads():
    """Proves EAwaitAll in scan_media fans out to all three read jobs."""

    scope = {
        **BASE_SCOPE,
        "read_albums": emitting_read_albums,
        "read_photos": emitting_read_photos,
        "read_videos": emitting_read_videos,
    }

    events = list(
        evaluate("mirror_workflow", ({"publish_d1": False},), scope, n_workers=4)
    )

    assert sorted(e for e in events if isinstance(e, str)) == [
        "read_albums",
        "read_photos",
        "read_videos",
    ]


def test_mirror_workflow_parallel_image_processing():
    """Proves upload_media fans out grey and mosaic computation in parallel."""

    scope = {
        **BASE_SCOPE,
        "compute_contrasting_grey": emitting_grey,
        "compute_image_mosaic": emitting_mosaic,
    }

    fpaths = ["a.jpg", "b.jpg"]
    events = list(
        evaluate("mirror_workflow", ({"fpaths": fpaths},), scope, n_workers=4)
    )

    tuples = [e for e in events if isinstance(e, tuple)]
    assert sorted(tuples) == [
        ("grey", "a.jpg"),
        ("grey", "b.jpg"),
        ("mosaic", "a.jpg"),
        ("mosaic", "b.jpg"),
    ]


def test_mirror_workflow_all_jobs_run_in_full_execution():
    """Proves every job in the workflow is dispatched when all options are enabled."""

    scope = {
        **BASE_SCOPE,
        "media_scan": emitting_media_scan,
        "read_albums": emitting_read_albums,
        "read_photos": emitting_read_photos,
        "read_videos": emitting_read_videos,
        "wikidata_scan": emitting_wikidata_scan,
        "compute_contrasting_grey": emitting_grey,
        "compute_image_mosaic": emitting_mosaic,
        "upload_missing_photos": emitting_upload_missing_photos,
        "upload_missing_videos": emitting_upload_missing_videos,
        "publish_artifacts": emitting_publish_artifacts,
        "build_website": emitting_build_website,
    }

    events = list(
        evaluate(
            "mirror_workflow",
            (
                {
                    "fpaths": ["a.jpg"],
                    "upload_images": True,
                    "upload_videos": True,
                    "publish_d1": True,
                },
            ),
            scope,
            n_workers=4,
        )
    )

    assert sorted(e for e in events if isinstance(e, str)) == [
        "build_website",
        "media_scan",
        "publish_artifacts",
        "read_albums",
        "read_photos",
        "read_videos",
        "upload_missing_photos",
        "upload_missing_videos",
        "wikidata_scan",
    ]
