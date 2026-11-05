This directory holds the admin dashboard SPA's production build, copied
from web/dashboard/dist/ by the Makefile's `dashboard` target via
STAGE_DASHBOARD_DIST_CMD before `go build` runs.

The README.txt placeholder exists so the directory is non-empty at
source-control time (so the //go:embed web/dist/* directive in
internal/api/web_handler.go compiles even before the SPA has been
built). The Makefile preserves this placeholder when staging a fresh build.

A plain `go build` embeds the files currently in this directory, including
the checked-in dashboard assets. Run `make dashboard` to refresh those
assets before building the server. If only this placeholder is present,
the SPA has no index.html and requests for the dashboard return 404.
