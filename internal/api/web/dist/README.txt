This directory holds the admin dashboard SPA's production build, copied
from web/dashboard/dist/ by the Makefile's `dashboard` target via
STAGE_DASHBOARD_DIST_CMD before `go build` runs.

The README.txt placeholder exists so the directory is non-empty at
source-control time (so the //go:embed all:dist directive in
internal/api/web_handler.go compiles even before the SPA has been
built). It is overwritten in place when the dashboard is rebuilt;
its content is not served.

If you ran `go build` directly without first running `make dashboard`,
this directory will still contain only this file. The WebHandler's
Register() will detect the empty dist and serve a 404 with a clear
remediation message ("dashboard not built; run make dashboard").
