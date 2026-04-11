from __future__ import annotations

import csv
import io
from typing import Any, Callable, Iterable, Optional

from fastapi.responses import StreamingResponse


class CsvStreamingResponse(StreamingResponse):
    def __init__(self, iterator_factory: Callable[[], Iterable[bytes]], **kwargs: Any) -> None:
        self._iterator_factory = iterator_factory
        super().__init__(iterator_factory(), **kwargs)

    def render_for_test(self) -> bytes:
        return b"".join(self._iterator_factory())


def parse_page_number(raw_value: Optional[str], default: int = 1) -> int:
    try:
        return max(int(raw_value or default), 1)
    except (TypeError, ValueError):
        return default


def build_page_context(*, items: list[Any], total_items: int, page: int, page_size: int) -> dict[str, Any]:
    normalized_page_size = max(int(page_size or 1), 1)
    normalized_total_items = max(int(total_items or 0), 0)
    total_pages = max((normalized_total_items + normalized_page_size - 1) // normalized_page_size, 1)
    normalized_page = min(max(int(page or 1), 1), total_pages)
    return {
        "items": items,
        "page": normalized_page,
        "page_size": normalized_page_size,
        "total_items": normalized_total_items,
        "total_pages": total_pages,
        "has_previous": normalized_page > 1,
        "has_next": normalized_page < total_pages,
        "previous_page": normalized_page - 1 if normalized_page > 1 else 1,
        "next_page": normalized_page + 1 if normalized_page < total_pages else total_pages,
    }


def fetch_page(
    fetcher: Callable[..., tuple[list[Any], int]],
    *,
    page: int,
    page_size: int,
) -> tuple[list[Any], dict[str, Any]]:
    normalized_page = max(int(page or 1), 1)
    normalized_page_size = max(int(page_size or 1), 1)
    offset = (normalized_page - 1) * normalized_page_size
    items, total_items = fetcher(limit=normalized_page_size, offset=offset)
    total_pages = max((max(int(total_items or 0), 0) + normalized_page_size - 1) // normalized_page_size, 1)
    if total_items and normalized_page > total_pages:
        normalized_page = total_pages
        offset = (normalized_page - 1) * normalized_page_size
        items, total_items = fetcher(limit=normalized_page_size, offset=offset)
    return items, build_page_context(
        items=items,
        total_items=total_items,
        page=normalized_page,
        page_size=normalized_page_size,
    )


def iter_all_pages(
    fetcher: Callable[..., tuple[list[Any], int]],
    *,
    page_size: int = 500,
):
    offset = 0
    normalized_page_size = max(int(page_size or 1), 1)
    while True:
        batch, total_items = fetcher(limit=normalized_page_size, offset=offset)
        if not batch:
            break
        for item in batch:
            yield item
        offset += len(batch)
        if offset >= max(int(total_items or 0), 0):
            break


def stream_csv(
    *,
    header: list[str],
    row_iterable: Iterable[list[str]],
    filename: str,
) -> CsvStreamingResponse:
    def iterator():
        yield "\ufeff".encode("utf-8")
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(header)
        yield buffer.getvalue().encode("utf-8")
        buffer.seek(0)
        buffer.truncate(0)
        for row in row_iterable:
            writer.writerow(row)
            yield buffer.getvalue().encode("utf-8")
            buffer.seek(0)
            buffer.truncate(0)

    return CsvStreamingResponse(
        iterator_factory=iterator,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
