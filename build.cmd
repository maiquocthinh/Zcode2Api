@echo off
REM Build the Svelte dashboard and the Go binary for release platforms.
REM Mirrors the Dockerfile: web/dist -> go:embed -> binary.
REM Artifacts go to build\ (gitignored).

echo Building Zcode2Api dashboard (web\dist)...
echo.

REM Reuse existing node_modules: npm ci wipes them, and a running editor
REM language server (e.g. svelte-language-server) holds the native rollup
REM .node file, which fails the wipe with EPERM on Windows. Install only
REM when the tree is missing or stale relative to package-lock.json.
pushd web
call npm ci --dry-run >NUL 2>&1
if %ERRORLEVEL% NEQ 0 (
	echo node_modules missing or stale - running npm ci...
	call npm ci
	if %ERRORLEVEL% NEQ 0 goto error
)
call npm run build
if %ERRORLEVEL% NEQ 0 goto error
popd

echo.
echo Building Zcode2Api binaries...
echo.

REM Stamp the release version from web\package.json.
for /f "tokens=2 delims=:," %%a in ('findstr /c:"\"version\":" web\package.json') do set VERSION=%%a
set VERSION=%VERSION:"=%
set VERSION=%VERSION: =%
set LDFLAGS=-s -w -X main.version=%VERSION%

if not exist "build" mkdir build

echo [1/4] Building Windows AMD64...
set GOOS=windows
set GOARCH=amd64
go build -trimpath -ldflags="%LDFLAGS%" -o build/zcode2api-windows-amd64.exe ./cmd/zcode2api
if %ERRORLEVEL% NEQ 0 goto error

echo [2/4] Building Linux AMD64...
set GOOS=linux
set GOARCH=amd64
go build -trimpath -ldflags="%LDFLAGS%" -o build/zcode2api-linux-amd64 ./cmd/zcode2api
if %ERRORLEVEL% NEQ 0 goto error

echo [3/4] Building macOS AMD64 (Intel)...
set GOOS=darwin
set GOARCH=amd64
go build -trimpath -ldflags="%LDFLAGS%" -o build/zcode2api-macos-amd64 ./cmd/zcode2api
if %ERRORLEVEL% NEQ 0 goto error

echo [4/4] Building macOS ARM64 (Apple Silicon)...
set GOOS=darwin
set GOARCH=arm64
go build -trimpath -ldflags="%LDFLAGS%" -o build/zcode2api-macos-arm64 ./cmd/zcode2api
if %ERRORLEVEL% NEQ 0 goto error

echo.
echo [32m✓ All builds completed successfully![0m
echo.
echo Final build artifacts:
dir /B build
echo.
echo [36mReady for GitHub release![0m
goto end

:error
echo.
echo [31m✗ Build failed![0m
exit /b 1

:end
